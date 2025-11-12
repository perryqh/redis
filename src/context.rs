use crate::{config::Config, store::Store};

pub struct AppContext<'a> {
    pub store: &'a Store,
    pub config: &'a Config,
}

impl<'a> AppContext<'a> {
    pub fn new(store: &'a Store, config: &'a Config) -> Self {
        Self { store, config }
    }
}
