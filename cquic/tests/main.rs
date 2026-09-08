use std::{net::SocketAddr, time::Duration};

use aok::{OK, Void};
use bytes::Bytes;
use wedb_quic::{Endpoint, MAX_RELIABLE_CHUNK};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[compio::test]
async fn test() -> Void {
  let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let server = Endpoint::bind(server_addr)?;
  let client = Endpoint::bind("127.0.0.1:0".parse().unwrap())?;

  // 服务端接入任务
  let accept_addr = server.local_addr();
  compio::runtime::spawn(async move {
    let (conn, remote) = server.accept().await.unwrap();
    log::info!("accepted {remote} cid {:#010x}", conn.id());
    // 回显服务：收到可靠消息原样返回
    while let Ok(msg) = conn.recv_reliable().await {
      if conn.send_reliable(msg).await.is_err() {
        break;
      }
    }
  });

  // 客户端连接与可靠往返
  let conn = client.connect(accept_addr).await?;
  assert!(!conn.remote_addr().ip().is_unspecified());

  // 大消息：跨多个分片（约 3.5 个分片）
  let payload: Vec<u8> = (0..MAX_RELIABLE_CHUNK * 7 / 2)
    .map(|i| (i % 251) as u8)
    .collect();
  conn.send_reliable(payload.clone()).await?;
  let echoed = compio::time::timeout(Duration::from_secs(5), conn.recv_reliable()).await??;
  assert_eq!(echoed, Bytes::from(payload), "可靠消息应原样往返");

  // 尽力而为数据报
  conn.send_datagram(Bytes::from_static(b"ping")).await?;

  // 多消息保序
  for i in 0..10u32 {
    conn
      .send_reliable(Bytes::from(i.to_le_bytes().to_vec()))
      .await?;
  }
  for i in 0..10u32 {
    let echoed = compio::time::timeout(Duration::from_secs(5), conn.recv_reliable()).await??;
    assert_eq!(echoed.as_ref(), i.to_le_bytes(), "消息应保序");
  }

  conn.close();
  OK
}
