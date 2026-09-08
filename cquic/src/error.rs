use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  /// 底层 IO 错误
  #[error(transparent)]
  Io(#[from] std::io::Error),
  /// 数据报解码失败
  #[error(transparent)]
  Decode(#[from] bitcode::Error),
  /// 连接已关闭
  #[error("connection closed")]
  Closed,
  /// 握手超时
  #[error("handshake timeout")]
  HandshakeTimeout,
  /// 参数非法（如数据报超出 MTU）
  #[error("invalid param: {0}")]
  InvalidParam(&'static str),
}

pub type Result<T> = result::Result<T, Error>;
