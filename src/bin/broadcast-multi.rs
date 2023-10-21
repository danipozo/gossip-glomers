use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
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
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    values: RwLock<BTreeSet<u64>>,
    topology: RwLock<BTreeMap<String, Vec<String>>>,
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
            let mut values = self.values.write().await;
            (*values).insert(request_value);
            let topology = self.topology.read().await;
            let neighbours = (*topology).get(runtime.node_id()).unwrap();
            for n in neighbours {
                let _ = runtime.rpc(n, request.body.clone()).await;
            }
            return runtime.reply(request, response_body).await;
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
