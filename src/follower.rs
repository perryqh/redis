use crate::context::AppContext;
use anyhow::Result;

pub struct Follower {
    app_context: AppContext,
}

impl Follower {
    pub fn new(app_context: AppContext) -> Self {
        Self { app_context }
    }

    pub async fn start(&self) -> Result<()> {
        // Implementation of follower logic
        Ok(())
    }
}
