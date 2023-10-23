use log::info;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::RwLock;
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
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    values: RwLock<BTreeSet<u64>>,
    topology: RwLock<BTreeMap<String, Vec<String>>>,
}

impl Handler {
    async fn send_request(runtime: Runtime, dest: String, body: MessageBody) {
        loop {
            let mut call = runtime.rpc(dest.clone(), body.clone()).await.unwrap();
            let (ctx, _handle) = Context::with_timeout(std::time::Duration::from_millis(200));
            let call_result = call.done_with(ctx).await;

            if let Ok(_) = call_result {
                info!("Call returned with success");
                break;
            }
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        if request.get_type() == "broadcast" {
            let response_body = MessageBody::new().with_type("broadcast_ok");
            let request_value = request.body.extra["message"]
                .as_u64()
                .ok_or("Non integer message")?;
            {
                let values = self.values.read().await;
                if (*values).contains(&request_value) {
                    return runtime.reply(request, response_body).await;
                }
            }
            let ret = runtime.reply(request.clone(), response_body).await;
            {
                let mut values = self.values.write().await;
                (*values).insert(request_value);
            }
            let topology = self.topology.read().await;
            let neighbours = (*topology).get(runtime.node_id()).unwrap();
            for n in neighbours.iter().filter(|n| **n != request.src) {
                tokio::spawn(Self::send_request(
                    runtime.clone(),
                    n.clone(),
                    request.body.clone(),
                ));
            }
            return ret;
        } else if request.get_type() == "read" {
            let mut response_body = MessageBody::new().with_type("read_ok");
            let values = self.values.read().await;
            response_body
                .extra
                .insert("messages".to_owned(), serde_json::json!(*values));
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
