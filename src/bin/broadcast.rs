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
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    values: RwLock<Vec<u64>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        if request.get_type() == "broadcast" {
            let response_body = MessageBody::new().with_type("broadcast_ok");
            let mut values = self.values.write().await;
            (*values).push(
                request.body.extra["message"]
                    .as_u64()
                    .ok_or("Non integer message")?,
            );
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
            return runtime.reply(request, response_body).await;
        }

        done(runtime, request)
    }
}
