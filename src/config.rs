use crate::cli::Args;
use anyhow::Result;

#[derive(Debug)]
pub struct Config {
    pub dir: String,
    pub dbfilename: String,
}

impl Config {
    pub fn new(args: Args) -> Result<Self> {
        Ok(Config {
            dir: args.dir,
            dbfilename: args.dbfilename,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dir: String::from("~/redis-rust"),
            dbfilename: String::from("dump.rdb"),
        }
    }
}
