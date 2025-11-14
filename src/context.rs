use crate::{config::Config, replication::Role, store::Store};

pub struct AppContext<'a> {
    pub store: &'a Store,
    pub config: &'a Config,
    pub replication_role: &'a Role,
}

impl<'a> AppContext<'a> {
    pub fn new(store: &'a Store, config: &'a Config, replication_role: &'a Role) -> Self {
        Self {
            store,
            config,
            replication_role,
        }
    }
}
