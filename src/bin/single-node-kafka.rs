use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    logs: RwLock<BTreeMap<String, Vec<u64>>>,
    offsets: RwLock<BTreeMap<String, u64>>,
    committed_offsets: RwLock<BTreeMap<String, u64>>,
}

impl Handler {
    fn new() -> Self {
        Handler {
            logs: Default::default(),
            offsets: Default::default(),
            committed_offsets: Default::default(),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        if request.get_type() == "send" {
            let mut response_body = MessageBody::new().with_type("send_ok");
            let request_key: String = request.body.extra["key"]
                .as_str()
                .ok_or("Non integer message")?
                .to_owned();
            let request_value: u64 = request.body.extra["msg"]
                .as_u64()
                .ok_or("Non integer message")?;

            {
                let mut logs = self.logs.write().await;
                let mut offsets = self.offsets.write().await;

                logs.entry(request_key.clone())
                    .or_insert(Vec::new())
                    .push(request_value);
                let offset = offsets.entry(request_key).or_insert(0);
                response_body
                    .extra
                    .insert("offset".to_owned(), serde_json::json!(offset));

                *offset += 1;
            }

            return runtime.reply(request.clone(), response_body).await;
        } else if request.get_type() == "poll" {
            let mut response_body = MessageBody::new().with_type("poll_ok");
            let request_offsets = request.body.extra["offsets"]
                .as_object()
                .ok_or("Incorrect message")?;

            let mut response_offsets: BTreeMap<String, Vec<(u64, u64)>> = BTreeMap::new();
            {
                let logs = self.logs.read().await;

                for (key, off) in request_offsets.iter() {
                    let off = off.as_u64().unwrap();
                    if let Some(l) = logs.get(key) {
                        response_offsets.insert(
                            key.clone(),
                            l.iter()
                                .skip(off as usize)
                                .take(5)
                                .enumerate()
                                .map(|(i, l)| (i as u64 + off, *l))
                                .collect(),
                        );
                    }
                }
            }

            response_body
                .extra
                .insert("msgs".to_owned(), json!(response_offsets));
            return runtime.reply(request, response_body).await;
        } else if request.get_type() == "commit_offsets" {
            let response_body = MessageBody::new().with_type("commit_offsets_ok");
            let request_offsets = request.body.extra["offsets"]
                .as_object()
                .ok_or("Incorrect message")?;

            {
                let mut committed_offsets = self.committed_offsets.write().await;
                for (key, off) in request_offsets.iter() {
                    let off = off.as_u64().unwrap();
                    *committed_offsets.entry(key.clone()).or_insert(off) = off;
                }
            }

            return runtime.reply(request, response_body).await;
        } else if request.get_type() == "list_committed_offsets" {
            let mut response_body = MessageBody::new().with_type("list_committed_offsets_ok");
            let request_keys = request.body.extra["keys"]
                .as_array()
                .ok_or("Incorrect message")?;
            let mut committed_offsets_response: BTreeMap<String, u64> = BTreeMap::new();
            {
                let committed_offsets = self.committed_offsets.read().await;
                for key in request_keys {
                    let key = key.as_str().unwrap();
                    if let Some(off) = committed_offsets.get(key) {
                        committed_offsets_response.insert(key.to_owned(), *off);
                    }
                }
            }

            response_body
                .extra
                .insert("offsets".to_owned(), json!(committed_offsets_response));
            return runtime.reply(request, response_body).await;
        }

        done(runtime, request)
    }
}
