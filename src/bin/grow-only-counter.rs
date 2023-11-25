use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio::sync::{mpsc::Receiver, RwLock};
use tokio_context::context::Context;

use async_trait::async_trait;
use maelstrom::{
    done,
    kv::{seq_kv, Storage, KV},
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
    value: RwLock<u64>,
    seq_kv: RwLock<Option<Storage>>,

    message_tx: mpsc::Sender<u64>,
    runtime_tx: mpsc::Sender<Storage>,
    sent_kv: RwLock<bool>,
}

impl Handler {
    fn new() -> Self {
        let (mtx, mrx) = mpsc::channel::<_>(1000);
        let (rtx, rrx) = mpsc::channel(1);

        tokio::spawn(Self::addition_batcher(mrx, rrx));

        Handler {
            message_tx: mtx,
            runtime_tx: rtx,
            sent_kv: RwLock::new(false),
            value: Default::default(),
            seq_kv: Option::None.into(),
        }
    }

    async fn addition_batcher(mut rmx: Receiver<u64>, mut rrx: mpsc::Receiver<Storage>) {
        // First of all receive the sequential kv store
        let kv = rrx.recv().await.unwrap();
        // Then make sure the key "counter" exists
        loop {
            let (ctx, _handle) = Context::with_timeout(std::time::Duration::from_millis(400));
            match kv.cas(ctx, "counter".to_owned(), 0, 0, true).await {
                Ok(_) => break,
                Err(e) => {
                    if let Some(error) = e.downcast_ref::<maelstrom::Error>() {
                        if *error == maelstrom::Error::PreconditionFailed {
                            break;
                        }
                    }
                }
            }
        }

        // Then start to work
        let mut message: u64 = 0;
        loop {
            // Receive as many messages as possible
            loop {
                match rmx.try_recv() {
                    Ok(v) => message += v,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            // Read the current counter value
            let (ctx, _handle) = Context::with_timeout(std::time::Duration::from_millis(100));
            let _ = match kv.get::<u64>(ctx, "counter".to_owned()).await {
                Ok(current) => {
                    let (ctx, _handle) =
                        Context::with_timeout(std::time::Duration::from_millis(100));
                    // Try to sum the accumulated delta to the counter. If it fails,
                    // just try on the next iteration
                    kv.cas(ctx, "counter".to_owned(), current, current + message, true)
                        .await
                        .and_then(|_| {
                            message = 0;
                            Ok(())
                        })
                }
                Err(e) => panic!("Error during get: {:?}", e),
            };

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        {
            let mut kv = self.seq_kv.write().await;
            if (*kv).is_none() {
                *kv = Some(seq_kv(runtime.clone()));
            }
        }
        {
            let mut sent_kv = self.sent_kv.write().await;
            if !*sent_kv {
                {
                    let kv = self.seq_kv.read().await;
                    let _ = self.runtime_tx.send((*kv).as_ref().unwrap().clone()).await;
                }
                *sent_kv = true;
            }
        }
        if request.get_type() == "add" {
            let response_body = MessageBody::new().with_type("add_ok");
            let request_value: u64 = request.body.extra["delta"]
                .as_u64()
                .ok_or("Non integer message")?;
            let ret = runtime.reply(request.clone(), response_body).await;

            {
                let mut value = self.value.write().await;
                *value += request_value;
            }
            let _ = self.message_tx.send(request_value).await;

            return ret;
        } else if request.get_type() == "read" {
            let mut response_body = MessageBody::new().with_type("read_ok");
            let (ctx, _handle) = Context::with_timeout(std::time::Duration::from_millis(100));
            let mut value;
            {
                let kv_guard = self.seq_kv.read().await;
                let kv = (*kv_guard).as_ref().unwrap();
                value = kv.get::<u64>(ctx, "counter".to_owned()).await.ok();
            }
            if value.is_none() {
                value = Some(*self.value.read().await);
            }
            response_body
                .extra
                .insert("value".to_owned(), serde_json::json!(value.unwrap()));
            return runtime.reply(request, response_body).await;
        }

        done(runtime, request)
    }
}
