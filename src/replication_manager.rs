use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::RwLock;

/// Manages follower connections and command propagation for a Redis leader
#[derive(Debug)]
pub struct ReplicationManager {
    followers: Arc<RwLock<Vec<FollowerConnection>>>,
}

/// Represents a connected follower
#[allow(dead_code)]
#[derive(Debug)]
struct FollowerConnection {
    id: String,
    writer: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
}

impl ReplicationManager {
    /// Creates a new ReplicationManager
    pub fn new() -> Self {
        Self {
            followers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Registers a new follower connection
    ///
    /// # Arguments
    /// * `writer` - The write half of the TCP stream to the follower
    ///
    /// # Returns
    /// The ID assigned to this follower
    pub async fn register_follower(&self, writer: OwnedWriteHalf) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let follower = FollowerConnection {
            id: id.clone(),
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
        };

        let mut followers = self.followers.write().await;
        followers.push(follower);

        eprintln!("Registered follower: {} (total: {})", id, followers.len());
        id
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
                }
            }
        }

        success_count
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

        let follower_id = manager.register_follower(writer).await;

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
