use std::{collections::HashMap, fmt::Display, sync::Mutex};

use async_trait::async_trait;
use common_macros::hash_map;
use distbench::{self, community::PeerId, Algorithm, SelfTerminating};
use log::{error, info};
use rand::Rng;
use tokio::time::{sleep, Duration};

#[distbench::message]
struct ElectionMessage {
    elector: u32,
}

#[distbench::message]
struct TerminationMessage {
    leader: u32,
}

#[distbench::state]
pub struct ChangRoberts {
    #[distbench::config]
    node_id: u32,

    leader: Mutex<Option<u32>>,
}

#[async_trait]
impl Algorithm for ChangRoberts {
    async fn on_start(&self) {
        let delay = rand::thread_rng().gen_range(1.0..3.0);
        sleep(Duration::from_secs_f64(delay)).await;

        if self.leader.lock().unwrap().is_some() {
            return;
        }

        if let Some((peer_id, peer)) = self.peers().next() {
            info!(
                "[Node {}] Starting by selecting a node: {}",
                self.node_id, peer_id
            );

            match peer
                .election(ElectionMessage {
                    elector: self.node_id,
                })
                .await
            {
                Ok(_) => {}
                Err(e) => error!(
                    "[Node {}] Error sending election message: {}",
                    self.node_id, e
                ),
            }
        }
    }

    async fn report(&self) -> Option<HashMap<impl Display, impl Display>> {
        Some(hash_map! {
            "leader" => self.leader.lock().unwrap().map_or("None".to_string(), |leader| leader.to_string()),
        })
    }
}

#[distbench::handlers]
impl ChangRoberts {
    async fn election(&self, _: PeerId, msg: &ElectionMessage) {
        if let Some((_, next_peer)) = self.peers().next() {
            info!(
                "[Node {}] Got a message with elector id: {}",
                self.node_id, msg.elector
            );
            if msg.elector == self.node_id {
                *self.leader.lock().unwrap() = Some(self.node_id);
                info!("[Node {}] we are elected!", self.node_id);
                info!(
                    "[Node {}] Sending message to terminate the algorithm!",
                    self.node_id
                );

                if let Err(e) = next_peer
                    .termination(&TerminationMessage {
                        leader: self.node_id,
                    })
                    .await
                {
                    error!(
                        "[Node {}] Error sending termination message: {}",
                        self.node_id, e
                    );
                }
                self.terminate().await;
            } else {
                if let Err(e) = next_peer
                    .election(&ElectionMessage {
                        elector: msg.elector.max(self.node_id),
                    })
                    .await
                {
                    error!(
                        "[Node {}] Error sending election message: {}",
                        self.node_id, e
                    );
                }
            }
        }
    }

    async fn termination(&self, _: PeerId, msg: &TerminationMessage) {
        if self.leader.lock().unwrap().is_some() {
            return;
        }

        if let Some((_, next_peer)) = self.peers().next() {
            *self.leader.lock().unwrap() = Some(msg.leader);
            if let Err(e) = next_peer.termination(msg).await {
                error!(
                    "[Node {}] Error sending termination message: {}",
                    self.node_id, e
                );
            }
        }

        self.terminate().await;
    }
}
