use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::{Node, Runtime, Result, protocol::Message, done};

use uuid::Uuid;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler;

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, request: Message) -> Result<()> {
        if request.get_type() == "generate" {
            let mut response_body = request.body.clone().with_type("generate_ok");
            response_body.extra.clear();
            response_body.extra.insert("id".to_owned(), serde_json::json!(Uuid::new_v4().simple().to_string()));
            return runtime.reply(request, response_body).await;
        }

        done(runtime, request)
    }
}
