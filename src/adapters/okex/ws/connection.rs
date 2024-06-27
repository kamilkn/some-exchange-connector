use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use compact_str::ToCompactString;
use eyre::{eyre, Result};
use log::{error, trace, warn};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::ports::connection::MdConnection;
use crate::common::ws::WebSocket;
use crate::adapters::okex::ws::config::OkexWsConnectionConfig;
use crate::adapters::okex::ws::model::{EventType, OkexWsDataMessage, OkexWsMessage};
use crate::adapters::okex::ws::stream::{OkexStream, OkexStreamKind};
use crate::domain::market_data::{MdMessage, Side};

pub struct OkexWsConnection {
    ws: WebSocket<OkexStream, OkexWsMessage>,
    increment_queue: VecDeque<MdMessage>,
    rx: Receiver<()>,
}

impl OkexWsConnection {
    /// If you need to subscribe to many 50 or 400 depth level channels,
    /// it is recommended to subscribe through multiple websocket connections, with each of less than 30 channels.
    /// https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel
    pub async fn new(tickers: Vec<impl ToCompactString>, config: OkexWsConnectionConfig) -> Result<Self> {
      let tickers: Vec<_> = tickers.into_iter().map(|s| s.to_compact_string()).collect();
      let stream = OkexStream { tickers, kind: OkexStreamKind::L2Update };
      let mut ws = WebSocket::try_establish_connection(
          &config.ws_url,
          stream,
          config.subscribe_interval_ms
      ).await.map_err(|e| eyre!("Failed to connect to Okex websocket: {:?}", e))?;
      
      ws.subscribe().await.map_err(|e| eyre!("Failed to subscribe to Okex md: {:?}", e))?;
      
      let (tx, rx) = mpsc::channel(1);
      tokio::spawn(Self::ping_task(tx, config.ping_frequency_seconds));

      Ok(Self {
          ws,
          increment_queue: VecDeque::new(),
          rx,
      })
    }

    async fn ping_task(tx: Sender<()>, frequency: u64) {
        let mut interval = tokio::time::interval(Duration::from_secs(frequency));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(_e) = tx.send(()).await {
                error!("Failed to send Ping to Okex channel");
            }
        }
    }
}

#[async_trait]
impl MdConnection for OkexWsConnection {
    async fn next(&mut self) -> Result<MdMessage> {
        while self.increment_queue.is_empty() {
            tokio::select! {
                res = self.ws.next() => {
                    let ws_message = res?;

                    match ws_message {
                        OkexWsMessage::Combined(combined) => {
                            if combined.message.len() != 1 {
                                return Err(eyre!("Failed to deserialize: The incoming message length does not equal 1. {combined:?}"));
                            }
                            match combined.message.first() {
                                Some(OkexWsDataMessage::BookSnapshot(snapshot)) => {
                                    let instrument_id = combined.arg.inst_id.clone();
                                    if let Some(prev_seq_id) = snapshot.prev_seq_id {
                                        if prev_seq_id != -1 {
                                            let bids = snapshot.bids
                                                .iter()
                                                .enumerate()
                                                .map(|(i, b)| b.to_md(
                                                    Some(snapshot.ts),
                                                    instrument_id.clone(),
                                                    Side::Bid,
                                                    snapshot.seq_id,
                                                    i + 1 == snapshot.bids.len() && snapshot.asks.is_empty(),
                                                ));
                                            let asks = snapshot.asks
                                                .iter()
                                                .enumerate()
                                                .map(|(i, a)| a.to_md(
                                                    Some(snapshot.ts),
                                                    instrument_id.clone(),
                                                    Side::Ask,
                                                    snapshot.seq_id,
                                                    i + 1 == snapshot.asks.len()
                                                ));
                                            self.increment_queue.extend(bids);
                                            self.increment_queue.extend(asks);
                                            break;
                                        }
                                    }
                                    return Ok(MdMessage::L2Snapshot(snapshot.to_internal_snapshot(instrument_id)));
                                },
                                _ => return Err(eyre!("Unexpected message type")),
                            }
                        },
                        OkexWsMessage::SubEvent(sub) => {
                            if matches!(sub.event, EventType::Error) {
                                warn!("received error subscribe event {sub:?}")
                            } else {
                                trace!("sub event");
                            }
                            continue;
                        }
                        OkexWsMessage::Pong => {
                            continue;
                        }
                    }
                }
                ping = self.rx.recv() => {
                    if let Some(_ping_msg) = ping {
                        let res = self.ws.ping(vec![]).await;
                        if res.is_err() {
                            error!("Failed to send Ping to Okex MD stream");
                        }
                    }
                }
            }
        }
        if let Some(update) = self.increment_queue.pop_front() {
            Ok(update)
        } else {
            Err(eyre!("Increment queue was unexpectedly empty"))
        }
    }
}
#[cfg(test)]
mod md_integration_tests {
    use std::time::Duration;
    use crate::ports::connection::MdConnection;
    use crate::adapters::okex::ws::config::OkexWsConnectionConfig;
    use crate::adapters::okex::ws::connection::OkexWsConnection;
    use crate::domain::storage::Storage;
    use eyre::Result;

    #[ignore]
    #[tokio::test]
    async fn ob_test() -> Result<()> {
        log4rs::init_file("logging.yaml", Default::default()).expect("logger initialisation failure");

        let mut storage = Storage::new();

        let mut connection = OkexWsConnection::new(
            vec!["BTC-USDT", "ETH-USDT"],
            OkexWsConnectionConfig::default(),
        ).await?;

        loop {
            let m = connection.next().await?;
            storage.on_ws_update(m);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
