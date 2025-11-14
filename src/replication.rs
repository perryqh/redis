use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum Role {
    Master(MasterReplication),
    Slave(SlaveReplication),
}

#[derive(Debug, Clone)]
pub struct MasterReplication {
    pub replication_id: String,
    pub replication_offset: u64,
}

#[derive(Debug, Clone)]
pub struct SlaveReplication {
    pub master_replication_id: String,
}

impl Default for MasterReplication {
    fn default() -> Self {
        let replication_id: String = format!("{}-{}", Uuid::new_v4(), Uuid::new_v4())
            .chars()
            .take(40)
            .collect();

        MasterReplication {
            replication_id,
            replication_offset: 0,
        }
    }
}

impl SlaveReplication {
    pub fn new(master_replication_id: String) -> Self {
        SlaveReplication {
            master_replication_id,
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
        let master_replication = MasterReplication::default();
        assert_eq!(master_replication.replication_id.len(), 40);
        assert_eq!(master_replication.replication_offset, 0);
    }

    #[test]
    fn test_slave_replication() {
        let slave_replication = SlaveReplication::new("master_replication_id".to_string());
        assert_eq!(
            slave_replication.master_replication_id,
            "master_replication_id"
        );
    }
}
