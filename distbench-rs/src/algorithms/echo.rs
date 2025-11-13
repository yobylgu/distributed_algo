use std::{
    collections::HashMap,
    fmt::Display,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use common_macros::hash_map;
use distbench::{self, community::PeerId, Algorithm, SelfTerminating};
use log::{error, info};

#[distbench::message]
struct Message {
    sender: String,
    message: String,
}

#[distbench::state]
pub struct Echo {
    #[distbench::config(default = false)]
    start_node: bool,
    messages_received: AtomicU64,
}

#[async_trait]
impl Algorithm for Echo {
    async fn on_start(&self) {
        info!("Echo algorithm starting");
        if self.start_node {
            for (_, peer) in self.peers() {
                match peer
                    .message(&Message {
                        sender: "Test".to_string(),
                        message: "Hello, world!".to_string(),
                    })
                    .await
                {
                    Ok(Some(message)) => info!("Message echoed: {}", message),
                    Ok(None) => error!("Message not echoed"),
                    Err(e) => error!("Error echoing message: {}", e),
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.terminate().await;
    }

    async fn on_exit(&self) {
        info!("Echo algorithm exiting");
    }

    async fn report(&self) -> Option<HashMap<impl Display, impl Display>> {
        Some(hash_map! {
            "messages_received" => self.messages_received.load(Ordering::Relaxed).to_string(),
        })
    }
}

#[distbench::handlers]
impl Echo {
    async fn message(&self, src: PeerId, msg: &Message) -> Option<String> {
        info!("Received message from {}: {}", src.to_string(), msg.message);
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        Some(msg.message.clone())
    }
}
