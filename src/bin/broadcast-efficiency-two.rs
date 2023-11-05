use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio::sync::{mpsc::Receiver, RwLock};
use tokio_context::context::Context;

use async_trait::async_trait;
use maelstrom::{
    done,
    protocol::{Message, MessageBody},
    Node, Result, Runtime,
};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::new());
    runtime.with_handler(handler).run().await
}

struct Handler {
    values: RwLock<BTreeSet<u64>>,
    topology: RwLock<BTreeMap<String, Vec<String>>>,

    message_tx: mpsc::Sender<(String, MessageBody)>,
    runtime_tx: mpsc::Sender<Runtime>,
    sent_runtime: RwLock<bool>,
}

impl Handler {
    fn new() -> Self {
        let (mtx, mrx) = mpsc::channel::<_>(1000);
        let (rtx, rrx) = mpsc::channel(1);

        tokio::spawn(Self::message_batcher(mrx, rrx));

        Handler {
            message_tx: mtx,
            runtime_tx: rtx,
            sent_runtime: RwLock::new(false),
            values: Default::default(),
            topology: Default::default(),
        }
    }

    async fn message_batcher(
        mut rmx: Receiver<(String, MessageBody)>,
        mut rrx: mpsc::Receiver<Runtime>,
    ) {
        // First of all receive the runtime
        let runtime = Arc::new(rrx.recv().await.unwrap());
        // Then start to work
        let mut messages: BTreeMap<String, (std::time::Instant, Vec<MessageBody>)> =
            BTreeMap::new();
        loop {
            // Receive as many messages as possible
            loop {
                match rmx.try_recv() {
                    Ok((dest, body)) => {
                        if let Some((_, batch)) = messages.get_mut(&dest) {
                            batch.push(body);
                        } else {
                            let mut new_vec = Vec::new();
                            new_vec.push(body);
                            messages.insert(dest, (std::time::Instant::now(), new_vec));
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            // Look at batches to send those that have been waiting for long enough
            let batches_send = messages
                .iter()
                .filter(|(_, (i, _))| i.elapsed() >= Duration::from_millis(1000))
                .map(|(k, _)| k.clone())
                .collect::<Vec<String>>();
            for dest in batches_send {
                let (_, msgs) = messages.remove(&dest).unwrap();
                let mut new_body = msgs[0].clone();
                new_body.extra["message"] = serde_json::json!(msgs
                    .iter()
                    .map(|x| x.extra["message"].as_array().unwrap().get(0).unwrap())
                    .collect::<Vec<_>>());
                tokio::spawn(Self::send_request(
                    runtime.clone(),
                    dest.to_string(),
                    new_body.clone(),
                ));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn send_request(runtime: Arc<Runtime>, dest: String, body: MessageBody) {
        loop {
            let mut call = runtime.rpc(dest.clone(), body.clone()).await.unwrap();
            let (ctx, _handle) = Context::with_timeout(std::time::Duration::from_millis(400));
            let call_result = call.done_with(ctx).await;

            if let Ok(_) = call_result {
                break;
            }
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        {
            let mut sent_runtime = self.sent_runtime.write().await;
            if !*sent_runtime {
                let _ = self.runtime_tx.send(runtime.clone()).await;
                *sent_runtime = true;
            }
        }
        if request.get_type() == "broadcast" {
            let response_body = MessageBody::new().with_type("broadcast_ok");
            let request_values: Vec<u64> = request.body.extra["message"]
                .as_u64()
                .and_then(|x| vec![x].into())
                .or_else(|| {
                    request.body.extra["message"]
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .map(|x| x.as_u64())
                        .collect()
                })
                .ok_or("Non integer message")?;
            let ret = runtime.reply(request.clone(), response_body).await;

            {
                let mut values = self.values.write().await;
                for v in &request_values {
                    (*values).insert(*v);
                }
            }
            if request.src.starts_with("n") {
                return ret;
            }
            let runtime = Arc::new(runtime);
            let mut new_body = MessageBody::new().with_type(request.body.typ);
            new_body
                .extra
                .insert("message".to_owned(), request_values.into());
            for n in runtime.neighbours() {
                let _ = self.message_tx.send((n.clone(), new_body.clone())).await;
            }
            return ret;
        } else if request.get_type() == "read" {
            let mut response_body = MessageBody::new().with_type("read_ok");
            {
                let values = self.values.read().await;
                response_body
                    .extra
                    .insert("messages".to_owned(), serde_json::json!(*values));
            }
            return runtime.reply(request, response_body).await;
        } else if request.get_type() == "topology" {
            let response_body = MessageBody::new().with_type("topology_ok");
            let mut topology = self.topology.write().await;
            *topology = BTreeMap::from_iter(
                request.body.extra["topology"]
                    .as_object()
                    .ok_or("topology message is not object")?
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.as_array()
                                .unwrap()
                                .iter()
                                .map(|x| x.as_str().unwrap().to_owned())
                                .collect(),
                        )
                    }),
            );
            return runtime.reply(request, response_body).await;
        }

        done(runtime, request)
    }
}
