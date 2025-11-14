use std::sync::Arc;

use crate::{config::Config, replication::Role, store::Store};

#[derive(Debug, Clone)]
pub struct AppContext {
    pub store: Arc<Store>,
    pub config: Arc<Config>,
    pub replication_role: Arc<Role>,
}

impl AppContext {
    pub fn new(store: Store, config: Config, replication_role: Role) -> Self {
        Self {
            store: Arc::new(store),
            config: Arc::new(config),
            replication_role: Arc::new(replication_role),
        }
    }

    pub fn from_arc(store: Arc<Store>, config: Arc<Config>, replication_role: Arc<Role>) -> Self {
        Self {
            store,
            config,
            replication_role,
        }
    }
}

impl Default for AppContext {
    fn default() -> Self {
        Self {
            store: Arc::new(Store::default()),
            config: Arc::new(Config::default()),
            replication_role: Arc::new(Role::default()),
        }
    }
}
