use std::{
    fmt::Debug,
    io::{Read, Write},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::task::{self, ArcWake, AtomicWaker};
use futures::{future::poll_fn, ready};
use http::Request;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::{
    error::{Error as WsError, ProtocolError, SubProtocolError},
    handshake::{
        client::{Response as WsResponse, generate_request},
        derive_accept_key,
        machine::{HandshakeMachine, RoundResult, StageResult},
    },
    protocol::{
        WebSocketConfig,
        frame::{
            Frame, FrameSocket,
            coding::{Control, Data, OpCode},
        },
    },
};

use crate::{
    common::errors::{map_io_error, new_io_error},
    proxy::AnyStream,
};

pub struct WebsocketConn {
    inner: FrameSocket<CompatStream<AnyStream>>,
    read_buffer: BytesMut,
    flush_pending: bool,
    read_closed: bool,
    write_closed: bool,
    continuation_expected: bool,
    max_frame_size: Option<usize>,
}

impl Debug for WebsocketConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketConn")
            .field("read_buffer", &self.read_buffer)
            .field("flush_pending", &self.flush_pending)
            .field("read_closed", &self.read_closed)
            .field("write_closed", &self.write_closed)
            .field("continuation_expected", &self.continuation_expected)
            .field("max_frame_size", &self.max_frame_size)
            .finish()
    }
}

impl WebsocketConn {
    pub async fn client(
        stream: AnyStream,
        request: Request<()>,
        config: Option<WebSocketConfig>,
    ) -> std::io::Result<(Self, WsResponse)> {
        let ws_config = config.unwrap_or_default();
        let (stream, response, tail) = client_handshake(stream, request).await?;
        Ok((Self::from_raw_stream(stream, tail, ws_config), response))
    }

    fn from_raw_stream(
        stream: AnyStream,
        tail: Vec<u8>,
        config: WebSocketConfig,
    ) -> Self {
        let inner = if tail.is_empty() {
            FrameSocket::new(CompatStream::new(stream))
        } else {
            FrameSocket::from_partially_read(CompatStream::new(stream), tail)
        };

        Self {
            inner,
            read_buffer: BytesMut::new(),
            flush_pending: false,
            read_closed: false,
            write_closed: false,
            continuation_expected: false,
            max_frame_size: config.max_frame_size,
        }
    }

    fn try_flush_socket(&mut self, cx: &mut Context<'_>) -> std::io::Result<()> {
        if !self.flush_pending {
            return Ok(());
        }

        self.inner
            .get_mut()
            .set_waker(ContextWaker::Write, cx.waker());
        match self.inner.flush() {
            Ok(()) => {
                self.flush_pending = false;
                Ok(())
            }
            Err(WsError::Io(err))
                if err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                Ok(())
            }
            Err(err) => Err(map_io_error(err)),
        }
    }

    fn poll_flush_socket(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.flush_pending {
            return Poll::Ready(Ok(()));
        }

        self.inner
            .get_mut()
            .set_waker(ContextWaker::Write, cx.waker());
        match self.inner.flush() {
            Ok(()) => {
                self.flush_pending = false;
                Poll::Ready(Ok(()))
            }
            Err(WsError::Io(err))
                if err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(map_io_error(err))),
        }
    }

    fn queue_frame(
        &mut self,
        cx: &mut Context<'_>,
        frame: Frame,
    ) -> std::io::Result<()> {
        self.inner
            .get_mut()
            .set_waker(ContextWaker::Write, cx.waker());

        let mut frame = frame;
        frame.header_mut().mask = Some(rand::random());

        match self.inner.write(frame) {
            Ok(()) => {
                self.flush_pending = true;
                Ok(())
            }
            Err(WsError::Io(err))
                if err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                self.flush_pending = true;
                Ok(())
            }
            Err(err) => Err(map_io_error(err)),
        }
    }

    fn poll_read_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<Option<Frame>>> {
        self.inner
            .get_mut()
            .set_waker(ContextWaker::Read, cx.waker());

        match self.inner.read(self.max_frame_size) {
            Ok(frame) => Poll::Ready(Ok(frame)),
            Err(WsError::Io(err))
                if err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(map_io_error(err))),
        }
    }

    fn validate_frame(&self, frame: &Frame) -> std::io::Result<()> {
        let header = frame.header();
        if header.rsv1 || header.rsv2 || header.rsv3 {
            return Err(map_io_error(WsError::Protocol(
                ProtocolError::NonZeroReservedBits,
            )));
        }
        if header.mask.is_some() {
            return Err(map_io_error(WsError::Protocol(
                ProtocolError::MaskedFrameFromServer,
            )));
        }

        if let OpCode::Control(_) = header.opcode {
            if !header.is_final {
                return Err(map_io_error(WsError::Protocol(
                    ProtocolError::FragmentedControlFrame,
                )));
            }
            if frame.payload().len() > 125 {
                return Err(map_io_error(WsError::Protocol(
                    ProtocolError::ControlFrameTooBig,
                )));
            }
        }

        Ok(())
    }

    fn handle_data_frame(
        &mut self,
        frame: Frame,
    ) -> std::io::Result<Option<bytes::Bytes>> {
        let header = frame.header().clone();
        match header.opcode {
            OpCode::Data(Data::Binary) => {
                if self.continuation_expected {
                    return Err(map_io_error(WsError::Protocol(
                        ProtocolError::ExpectedFragment(Data::Binary),
                    )));
                }
                self.continuation_expected = !header.is_final;
                Ok(Some(frame.into_payload()))
            }
            OpCode::Data(Data::Continue) => {
                if !self.continuation_expected {
                    return Err(map_io_error(WsError::Protocol(
                        ProtocolError::UnexpectedContinueFrame,
                    )));
                }
                self.continuation_expected = !header.is_final;
                Ok(Some(frame.into_payload()))
            }
            OpCode::Data(Data::Text) => Err(new_io_error("ws invalid message type")),
            OpCode::Data(Data::Reserved(op)) => Err(map_io_error(
                WsError::Protocol(ProtocolError::UnknownDataFrameType(op)),
            )),
            _ => Ok(None),
        }
    }

    fn handle_control_frame(
        &mut self,
        cx: &mut Context<'_>,
        frame: Frame,
    ) -> std::io::Result<()> {
        match frame.header().opcode {
            OpCode::Control(Control::Ping) => {
                if !self.write_closed {
                    self.queue_frame(cx, Frame::pong(frame.into_payload()))?;
                    self.try_flush_socket(cx)?;
                }
            }
            OpCode::Control(Control::Pong) => {}
            OpCode::Control(Control::Close) => {
                self.read_closed = true;
                if !self.write_closed {
                    self.write_closed = true;
                    self.queue_frame(cx, Frame::close(None))?;
                    self.try_flush_socket(cx)?;
                }
            }
            OpCode::Control(Control::Reserved(op)) => {
                return Err(map_io_error(WsError::Protocol(
                    ProtocolError::UnknownControlFrameType(op),
                )));
            }
            _ => unreachable!("not a control frame"),
        }

        Ok(())
    }
}

impl AsyncRead for WebsocketConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            if !self.read_buffer.is_empty() {
                let to_read = std::cmp::min(buf.remaining(), self.read_buffer.len());
                let chunk = self.read_buffer.split_to(to_read);
                buf.put_slice(&chunk[..to_read]);
                return Poll::Ready(Ok(()));
            }

            if self.read_closed {
                return Poll::Ready(Ok(()));
            }

            self.try_flush_socket(cx)?;

            let frame = match ready!(self.poll_read_frame(cx))? {
                Some(frame) => frame,
                None => {
                    self.read_closed = true;
                    return Poll::Ready(Ok(()));
                }
            };

            self.validate_frame(&frame)?;

            match frame.header().opcode {
                OpCode::Control(_) => {
                    self.handle_control_frame(cx, frame)?;
                }
                OpCode::Data(_) => {
                    if let Some(payload) = self.handle_data_frame(frame)? {
                        if payload.is_empty() {
                            continue;
                        }

                        let to_read = std::cmp::min(buf.remaining(), payload.len());
                        buf.put_slice(&payload[..to_read]);
                        if to_read < payload.len() {
                            self.read_buffer.extend_from_slice(&payload[to_read..]);
                        }
                        return Poll::Ready(Ok(()));
                    }
                }
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

        if self.write_closed {
            return Poll::Ready(Err(new_io_error("ws closed")));
        }

        self.try_flush_socket(cx)?;
        self.queue_frame(
            cx,
            Frame::message(buf.to_vec(), OpCode::Data(Data::Binary), true),
        )?;

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.poll_flush_socket(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if !self.write_closed {
            self.write_closed = true;
            self.queue_frame(cx, Frame::close(None))?;
        }

        ready!(self.poll_flush_socket(cx))?;
        Poll::Ready(Ok(()))
    }
}

async fn client_handshake(
    stream: AnyStream,
    request: Request<()>,
) -> std::io::Result<(AnyStream, WsResponse, Vec<u8>)> {
    let subprotocols = extract_subprotocols(&request)?;
    let (request, key) = generate_request(request).map_err(map_io_error)?;
    let accept_key = derive_accept_key(key.as_bytes());
    let mut machine = Some(HandshakeMachine::start_write(
        CompatStream::new(stream),
        request,
    ));

    poll_fn(move |cx| {
        loop {
            let mut current =
                machine.take().expect("handshake polled after completion");
            current.get_mut().set_waker(ContextWaker::Read, cx.waker());
            current.get_mut().set_waker(ContextWaker::Write, cx.waker());

            match current.single_round::<WsResponse>().map_err(map_io_error)? {
                RoundResult::WouldBlock(next) => {
                    machine = Some(next);
                    return Poll::Pending;
                }
                RoundResult::Incomplete(next) => {
                    machine = Some(next);
                }
                RoundResult::StageFinished(StageResult::DoneWriting(stream)) => {
                    machine = Some(HandshakeMachine::start_read(stream));
                }
                RoundResult::StageFinished(StageResult::DoneReading {
                    result,
                    stream,
                    tail,
                }) => {
                    verify_response(&result, &accept_key, &subprotocols)?;
                    return Poll::Ready(Ok((stream.into_inner(), result, tail)));
                }
            }
        }
    })
    .await
}

fn extract_subprotocols(
    request: &Request<()>,
) -> std::io::Result<Option<Vec<String>>> {
    request
        .headers()
        .get("Sec-WebSocket-Protocol")
        .map(|subprotocols| {
            subprotocols
                .to_str()
                .map(|value| {
                    value
                        .split(',')
                        .map(|part| part.trim().to_string())
                        .collect()
                })
                .map_err(map_io_error)
        })
        .transpose()
}

fn verify_response(
    response: &WsResponse,
    accept_key: &str,
    subprotocols: &Option<Vec<String>>,
) -> std::io::Result<()> {
    let headers = response.headers();

    if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(new_io_error("msg: websocket handshake failed"));
    }

    if !headers
        .get("Upgrade")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
    {
        return Err(map_io_error(WsError::Protocol(
            ProtocolError::MissingUpgradeWebSocketHeader,
        )));
    }

    if !headers
        .get("Connection")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("Upgrade"))
        .unwrap_or(false)
    {
        return Err(map_io_error(WsError::Protocol(
            ProtocolError::MissingConnectionUpgradeHeader,
        )));
    }

    if !headers
        .get("Sec-WebSocket-Accept")
        .map(|value| value == accept_key)
        .unwrap_or(false)
    {
        return Err(map_io_error(WsError::Protocol(
            ProtocolError::SecWebSocketAcceptKeyMismatch,
        )));
    }

    if headers.get("Sec-WebSocket-Protocol").is_none() && subprotocols.is_some() {
        return Err(map_io_error(WsError::Protocol(
            ProtocolError::SecWebSocketSubProtocolError(
                SubProtocolError::NoSubProtocol,
            ),
        )));
    }

    if headers.get("Sec-WebSocket-Protocol").is_some() && subprotocols.is_none() {
        return Err(map_io_error(WsError::Protocol(
            ProtocolError::SecWebSocketSubProtocolError(
                SubProtocolError::ServerSentSubProtocolNoneRequested,
            ),
        )));
    }

    if let Some(returned) = headers.get("Sec-WebSocket-Protocol") {
        if let Some(accepted) = subprotocols {
            if !accepted
                .contains(&returned.to_str().map_err(map_io_error)?.to_string())
            {
                return Err(map_io_error(WsError::Protocol(
                    ProtocolError::SecWebSocketSubProtocolError(
                        SubProtocolError::InvalidSubProtocol,
                    ),
                )));
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ContextWaker {
    Read,
    Write,
}

#[derive(Debug)]
struct CompatStream<S> {
    inner: S,
    write_waker_proxy: Arc<WakerProxy>,
    read_waker_proxy: Arc<WakerProxy>,
}

impl<S> CompatStream<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            write_waker_proxy: Default::default(),
            read_waker_proxy: Default::default(),
        }
    }

    fn set_waker(&self, kind: ContextWaker, waker: &task::Waker) {
        match kind {
            ContextWaker::Read => {
                self.write_waker_proxy.read_waker.register(waker);
                self.read_waker_proxy.read_waker.register(waker);
            }
            ContextWaker::Write => {
                self.write_waker_proxy.write_waker.register(waker);
                self.read_waker_proxy.write_waker.register(waker);
            }
        }
    }

    fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> CompatStream<S>
where
    S: Unpin,
{
    fn with_context<F, R>(
        &mut self,
        kind: ContextWaker,
        f: F,
    ) -> Poll<std::io::Result<R>>
    where
        F: FnOnce(&mut Context<'_>, Pin<&mut S>) -> Poll<std::io::Result<R>>,
    {
        let waker = match kind {
            ContextWaker::Read => task::waker_ref(&self.read_waker_proxy),
            ContextWaker::Write => task::waker_ref(&self.write_waker_proxy),
        };
        let mut context = task::Context::from_waker(&waker);
        f(&mut context, Pin::new(&mut self.inner))
    }
}

impl<S> Read for CompatStream<S>
where
    S: AsyncRead + Unpin,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut buf = ReadBuf::new(buf);
        match self.with_context(ContextWaker::Read, |ctx, stream| {
            stream.poll_read(ctx, &mut buf)
        }) {
            Poll::Ready(Ok(())) => Ok(buf.filled().len()),
            Poll::Ready(Err(err)) => Err(err),
            Poll::Pending => {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        }
    }
}

impl<S> Write for CompatStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.with_context(ContextWaker::Write, |ctx, stream| {
            stream.poll_write(ctx, buf)
        }) {
            Poll::Ready(result) => result,
            Poll::Pending => {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self
            .with_context(ContextWaker::Write, |ctx, stream| stream.poll_flush(ctx))
        {
            Poll::Ready(result) => result,
            Poll::Pending => {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        }
    }
}

#[derive(Debug, Default)]
struct WakerProxy {
    read_waker: AtomicWaker,
    write_waker: AtomicWaker,
}

impl ArcWake for WakerProxy {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.read_waker.wake();
        arc_self.write_waker.wake();
    }
}
