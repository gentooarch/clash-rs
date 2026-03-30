use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream, ready};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use crate::{
    common::errors::{map_io_error, new_io_error},
    proxy::AnyStream,
};

pub struct WebsocketConn {
    inner: WebSocketStream<AnyStream>,
    read_buffer: BytesMut,
}

impl Debug for WebsocketConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketConn")
            .field("read_buffer", &self.read_buffer)
            .finish()
    }
}

impl WebsocketConn {
    pub fn from_websocket(stream: WebSocketStream<AnyStream>) -> Self {
        Self {
            inner: stream,
            read_buffer: BytesMut::new(),
        }
    }
}

impl AsyncRead for WebsocketConn {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if !self.read_buffer.is_empty() {
                let to_read = std::cmp::min(buf.remaining(), self.read_buffer.len());
                let for_read = self.read_buffer.split_to(to_read);
                buf.put_slice(&for_read[..to_read]);
                return Poll::Ready(Ok(()));
            }

            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(Message::Binary(data))) => {
                    if data.is_empty() {
                        continue;
                    }

                    let to_read = std::cmp::min(buf.remaining(), data.len());
                    buf.put_slice(&data[..to_read]);
                    if to_read < data.len() {
                        self.read_buffer.extend_from_slice(&data[to_read..]);
                    }
                    return Poll::Ready(Ok(()));
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Ok(Message::Text(_))) => {
                    return Poll::Ready(Err(new_io_error(
                        "ws invalid message type",
                    )));
                }
                Some(Ok(_)) => continue,
                Some(Err(err)) => return Poll::Ready(Err(map_io_error(err))),
            }
        }
    }
}

impl AsyncWrite for WebsocketConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        ready!(Pin::new(&mut self.inner).poll_ready(cx)).map_err(map_io_error)?;
        let message = Message::Binary(Bytes::copy_from_slice(buf));
        Pin::new(&mut self.inner)
            .start_send(message)
            .map_err(map_io_error)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let Self { inner, .. } = self.get_mut();
        Pin::new(inner).poll_flush(cx).map_err(map_io_error)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let Self { inner, .. } = self.get_mut();
        let mut pin = Pin::new(inner);

        let message = Message::Close(None);
        #[allow(unused_must_use)]
        {
            pin.as_mut().start_send(message);
        }
        pin.poll_close(cx).map_err(map_io_error)
    }
}
