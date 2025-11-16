use std::sync::Arc;

use crate::{
    config::Config, replication::ReplicationRole, replication_manager::ReplicationManager,
    store::Store,
};

#[derive(Debug, Clone)]
pub struct AppContext {
    pub store: Arc<Store>,
    pub config: Arc<Config>,
    pub replication_role: Arc<ReplicationRole>,
    pub replication_manager: Option<Arc<ReplicationManager>>,
}

impl AppContext {
    pub fn new(store: Store, config: Config, replication_role: ReplicationRole) -> Self {
        let replication_manager = if replication_role.is_leader() {
            Some(Arc::new(ReplicationManager::new()))
        } else {
            None
        };

        Self {
            store: Arc::new(store),
            config: Arc::new(config),
            replication_role: Arc::new(replication_role),
            replication_manager,
        }
    }

    pub fn from_arc(
        store: Arc<Store>,
        config: Arc<Config>,
        replication_role: Arc<ReplicationRole>,
    ) -> Self {
        let replication_manager = if replication_role.is_leader() {
            Some(Arc::new(ReplicationManager::new()))
        } else {
            None
        };

        Self {
            store,
            config,
            replication_role,
            replication_manager,
        }
    }

    pub fn is_follower(&self) -> bool {
        self.replication_role.is_follower()
    }

    pub fn is_leader(&self) -> bool {
        self.replication_role.is_leader()
    }
}

impl Default for AppContext {
    fn default() -> Self {
        Self {
            store: Arc::new(Store::default()),
            config: Arc::new(Config::default()),
            replication_role: Arc::new(ReplicationRole::default()),
            replication_manager: Some(Arc::new(ReplicationManager::new())),
        }
    }
}
