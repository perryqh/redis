use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{timeout, Duration};

/// Manages follower connections and command propagation for a Redis leader
#[derive(Debug)]
pub struct ReplicationManager {
    followers: Arc<RwLock<Vec<FollowerConnection>>>,
    master_offset: AtomicU64,
}

/// Represents a connected follower
#[derive(Debug)]
struct FollowerConnection {
    id: String,
    bytes_written: AtomicU64,
    writer: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    #[allow(dead_code)] // Must be kept alive to maintain channel
    ack_sender: mpsc::UnboundedSender<u64>,
    ack_receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<u64>>>,
}

impl ReplicationManager {
    /// Creates a new ReplicationManager
    pub fn new() -> Self {
        Self {
            followers: Arc::new(RwLock::new(Vec::new())),
            master_offset: AtomicU64::new(0),
        }
    }

    pub async fn number_of_zero_byte_sent_followers(&self) -> usize {
        let followers = self.followers.read().await;
        followers
            .iter()
            .filter(|follower| follower.bytes_written.load(Ordering::Relaxed) == 0)
            .count()
    }

    /// Registers a new follower connection
    ///
    /// # Arguments
    /// * `writer` - The write half of the TCP stream to the follower
    ///
    /// # Returns
    /// A tuple of (follower_id, sender for ACK messages)
    pub async fn register_follower(
        &self,
        writer: OwnedWriteHalf,
    ) -> (String, mpsc::UnboundedSender<u64>) {
        let id = uuid::Uuid::new_v4().to_string();
        let (ack_sender, ack_receiver) = mpsc::unbounded_channel();
        let ack_sender_clone = ack_sender.clone();

        let follower = FollowerConnection {
            id: id.clone(),
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            bytes_written: AtomicU64::new(0),
            ack_sender,
            ack_receiver: Arc::new(tokio::sync::Mutex::new(ack_receiver)),
        };

        let mut followers = self.followers.write().await;
        followers.push(follower);

        eprintln!("Registered follower: {} (total: {})", id, followers.len());

        (id, ack_sender_clone)
    }

    /// Propagates a write command to all connected followers
    ///
    /// # Arguments
    /// * `command_bytes` - The raw RESP-encoded command to send
    ///
    /// # Returns
    /// Number of followers that successfully received the command
    pub async fn propagate_write(&self, command_bytes: &[u8]) -> usize {
        let followers = self.followers.read().await;
        let mut success_count = 0;

        for follower in followers.iter() {
            // Try to acquire the writer lock without blocking
            if let Ok(mut writer) = follower.writer.try_lock() {
                if writer.write_all(command_bytes).await.is_ok() && writer.flush().await.is_ok() {
                    success_count += 1;
                    follower
                        .bytes_written
                        .fetch_add(command_bytes.len() as u64, Ordering::Relaxed);
                    self.master_offset
                        .fetch_add(command_bytes.len() as u64, Ordering::Relaxed);
                }
            }
        }

        success_count
    }

    /// Waits for replicas to acknowledge write commands
    ///
    /// # Arguments
    /// * `num_replicas` - The minimum number of replicas to wait for
    /// * `timeout_ms` - The maximum time to wait in milliseconds
    ///
    /// # Returns
    /// The number of replicas that acknowledged the writes
    pub async fn wait_for_replicas(&self, num_replicas: u32, timeout_ms: u32) -> usize {
        let followers = self.followers.read().await;
        let num_followers = followers.len();

        if num_followers == 0 {
            return 0;
        }

        // Get the current master offset
        let current_offset = self.master_offset.load(Ordering::Relaxed);

        // If no writes have been sent, return the count of all connected followers
        if current_offset == 0 {
            return num_followers;
        }

        // Send REPLCONF GETACK * to all followers
        let getack_command = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        let mut ack_receivers = Vec::new();

        for follower in followers.iter() {
            let mut writer = follower.writer.lock().await;
            if writer.write_all(getack_command).await.is_ok() && writer.flush().await.is_ok() {
                // Clone the receiver Arc to listen for ACKs
                ack_receivers.push((
                    follower.id.clone(),
                    follower.ack_receiver.clone(),
                    follower.bytes_written.load(Ordering::Relaxed),
                ));
            }
        }

        drop(followers); // Release the read lock

        // Wait for ACKs with timeout
        let wait_duration = Duration::from_millis(timeout_ms as u64);

        // For each follower that we sent GETACK to, wait for their response
        let ack_futures: Vec<_> = ack_receivers
            .into_iter()
            .map(|(id, receiver_arc, expected_offset)| async move {
                let mut receiver = receiver_arc.lock().await;
                match timeout(wait_duration, receiver.recv()).await {
                    Ok(Some(offset)) => {
                        eprintln!(
                            "Follower {} ACK: offset={}, expected={}",
                            id, offset, expected_offset
                        );
                        offset >= expected_offset
                    }
                    Ok(None) => {
                        eprintln!("Follower {} channel closed", id);
                        false
                    }
                    Err(_) => {
                        eprintln!("Follower {} ACK timeout", id);
                        false
                    }
                }
            })
            .collect();

        // Wait for all ACK futures
        let results = futures::future::join_all(ack_futures).await;
        let acknowledged_count = results.iter().filter(|&&acked| acked).count();

        eprintln!(
            "WAIT complete: {}/{} replicas acknowledged (needed {})",
            acknowledged_count,
            results.len(),
            num_replicas
        );

        acknowledged_count
    }

    /// Returns the number of currently connected followers
    pub async fn follower_count(&self) -> usize {
        self.followers.read().await.len()
    }

    /// Removes disconnected followers
    /// This should be called periodically or when propagation fails
    pub async fn cleanup_disconnected(&self) -> usize {
        let mut followers = self.followers.write().await;
        let initial_count = followers.len();

        // Keep only followers that can be written to
        followers.retain(|_follower| {
            // For now, keep all followers
            // In a real implementation, we'd check if the connection is still alive
            true
        });

        let removed = initial_count - followers.len();
        if removed > 0 {
            eprintln!("Cleaned up {} disconnected followers", removed);
        }
        removed
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_manager_new() {
        let manager = ReplicationManager::new();
        assert_eq!(manager.follower_count().await, 0);
    }

    #[tokio::test]
    async fn test_register_follower() {
        let manager = ReplicationManager::new();

        // Create a mock TCP connection using a real TcpStream
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_task =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (server, _) = listener.accept().await.unwrap();
        let (_, writer) = server.into_split();

        let (follower_id, _sender) = manager.register_follower(writer).await;

        assert!(!follower_id.is_empty());
        assert_eq!(manager.follower_count().await, 1);

        drop(client_task); // Clean up
    }

    #[tokio::test]
    async fn test_register_multiple_followers() {
        let manager = ReplicationManager::new();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client1_task =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (server1, _) = listener.accept().await.unwrap();
        let (_, writer1) = server1.into_split();
        manager.register_follower(writer1).await;

        let client2_task =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
        let (server2, _) = listener.accept().await.unwrap();
        let (_, writer2) = server2.into_split();
        manager.register_follower(writer2).await;

        assert_eq!(manager.follower_count().await, 2);

        drop(client1_task);
        drop(client2_task);
    }

    #[tokio::test]
    async fn test_follower_count() {
        let manager = ReplicationManager::new();
        assert_eq!(manager.follower_count().await, 0);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_task =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (server, _) = listener.accept().await.unwrap();
        let (_, writer) = server.into_split();
        manager.register_follower(writer).await;

        assert_eq!(manager.follower_count().await, 1);

        drop(client_task);
    }
}
