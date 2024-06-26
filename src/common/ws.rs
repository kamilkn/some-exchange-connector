use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use compact_str::CompactString;
use eyre::bail;
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use http::StatusCode;
use log::{error, trace, warn};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream};

use crate::ports::connection::WsMessage;
use crate::ports::stream::WsStream;

pub type WebSocketStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct WebSocket<S, M>
where
    S: WsStream + Send + 'static + Clone,
{
    ws_stream: WebSocketStream,
    stream: S,
    pub ws_url: CompactString,
    subscribe_interval_ms: u64,
    _phantom_m: PhantomData<M>,
}
impl<S, M> WebSocket<S, M>
where
    S: WsStream + Send + 'static + Clone,
    <S as WsStream>::Subscribe: Debug,
    M: for<'de> Deserialize<'de> + WsMessage,
{
    pub async fn try_establish_connection(
        url: &CompactString,
        stream: S,
        subscribe_interval_ms: u64,
    ) -> eyre::Result<Self> {
        let (ws_stream, _response) = Self::connect(url).await?;
        Ok(Self {
            ws_stream,
            stream,
            ws_url: url.clone(),
            subscribe_interval_ms,
            _phantom_m: Default::default(),
        })
    }

    async fn connect(url: &CompactString) -> eyre::Result<(WebSocketStream, Response)> {
        let (stream, response) = connect_async(url.as_str()).await?;
        trace!(
            "WebSocket connection established, response = {:?}",
            response
        );
        Ok((stream, response))
    }

    pub async fn reconnect(&mut self) -> eyre::Result<()> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Err(err) = self.ws_stream.close(None).await {
            warn!("WebSocket stream close failure while reconnecting: {}", err);
        }
        trace!("Trying to reconnect to {}", self.ws_url);
        let (ws_stream, _response) = Self::connect(&self.ws_url).await?;
        trace!("Reconnected to {}", self.ws_url);
        self.ws_stream = ws_stream;
        self.subscribe().await?;
        trace!("Resubscribed to URL {}", &self.ws_url);
        Ok(())
    }

    pub async fn subscribe(&mut self) -> eyre::Result<()> {
        for subscribe_request in self.stream.subscribe_requests() {
            let string = serde_json::to_string(&subscribe_request)?;
            self.ws_stream.send(Message::text(string)).await?;
            if self.subscribe_interval_ms != 0 {
                tokio::time::sleep(Duration::from_millis(self.subscribe_interval_ms)).await;
            }
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Message, Error> {
        if let Some(result) = self.ws_stream.next().await {
            match result {
                Ok(r) => Ok(r),
                Err(err) => match err {
                    Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                        warn!("{}; reconnecting to {}", err, self.ws_url);
                        Ok(Message::Close(None))
                    }
                    Error::Http(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
                        error!("HTTP status: TOO_MANY_REQUESTS => 1min sleep before reconnecting to {}",  self.ws_url);
                        Ok(Message::Close(None))
                    }
                    _ => Err(err),
                },
            }
        } else {
            warn!(
                "IO error: An existing connection was forcibly closed by the remote host.\
                (os error 10054) - next() returned None; stream url = {:?}; reconnecting...",
                self.ws_url
            );
            Ok(Message::Close(None))
        }
    }

    pub async fn next(&mut self) -> eyre::Result<M> {
        loop {
            let message = self.recv().await?;
            return match message {
                Message::Text(s) => Ok(serde_json::from_str::<M>(&s)?),
                Message::Binary(data) => Ok(serde_json::from_slice::<M>(&data)?),
                Message::Close(_) => {
                    self.reconnect().await?;
                    continue;
                }
                Message::Pong(_) => Ok(M::pong()),
                _ => bail!("Unsupported WebSocket message"),
            };
        }
    }

    pub async fn ping(&mut self, data: Vec<u8>) -> eyre::Result<()> {
        self.ws_stream.send(Message::Ping(data.clone())).await?;
        Ok(())
    }
}
