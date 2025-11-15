use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum ReplicationRole {
    Leader(LeaderReplication),
    Follower(FollowerReplication),
}

impl Default for ReplicationRole {
    fn default() -> Self {
        ReplicationRole::Leader(LeaderReplication::default())
    }
}

impl ReplicationRole {
    pub fn is_follower(&self) -> bool {
        matches!(self, ReplicationRole::Follower(_))
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, ReplicationRole::Leader(_))
    }
}

#[derive(Debug, Clone)]
pub struct LeaderReplication {
    pub replication_id: String,
    pub replication_offset: u64,
}

#[derive(Debug, Clone)]
pub struct FollowerReplication {
    pub leader_host: String,
    pub leader_port: u16,
}

impl Default for LeaderReplication {
    fn default() -> Self {
        let replication_id: String = format!("{}-{}", Uuid::new_v4(), Uuid::new_v4())
            .chars()
            .take(40)
            .collect();

        LeaderReplication {
            replication_id,
            replication_offset: 0,
        }
    }
}

impl FollowerReplication {
    pub fn new(leader_host: String, leader_port: u16) -> Self {
        FollowerReplication {
            leader_host,
            leader_port,
        }
    }
}

impl Default for FollowerReplication {
    fn default() -> Self {
        FollowerReplication {
            leader_host: "127.0.0.1".to_string(),
            leader_port: 6379,
        }
    }
}

// # Replication
// role:master
// connected_slaves:0
//
// This ID identifies the current history of the master's dataset. When a master server boots for the first time or restarts, it resets its ID.
// - pseudo-random alphanumeric string of 40 characters
// master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
//
// The replication offset tracks the number of bytes of commands the master has streamed to its replicas. This value is used to update the state of the replicas with changes made to the dataset. The offset starts at 0 when a master boots up and no replicas have connected yet.
// master_repl_offset:0
// second_repl_offset:-1
// repl_backlog_active:0
// repl_backlog_size:1048576
// repl_backlog_first_byte_offset:0
// repl_backlog_histlen:
//
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_master_replication() {
        let master_replication = LeaderReplication::default();
        assert_eq!(master_replication.replication_id.len(), 40);
        assert_eq!(master_replication.replication_offset, 0);
    }

    #[test]
    fn test_follower_replication() {
        let follower_replication = FollowerReplication::new("master_host".to_string(), 6379);
        assert_eq!(follower_replication.leader_host, "master_host");
        assert_eq!(follower_replication.leader_port, 6379);
    }
}
