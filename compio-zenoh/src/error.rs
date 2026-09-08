use thiserror::Error;
use std::result;

#[derive(Error, Debug)]
pub enum Error {
  // #[error(transparent)]
}

pub type Result<T> = result::Result<T, Error>;

