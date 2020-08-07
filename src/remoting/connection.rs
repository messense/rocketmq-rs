use std::collections::HashMap;
use std::pin::Pin;

use futures::{
    task::{Context, Poll},
    Future, Sink, SinkExt, Stream, StreamExt,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::error::{ConnectionError, Error};
use crate::protocol::{MqCodec, RemoteCommand};

pub struct ConnectionSender {
    tx: mpsc::UnboundedSender<RemoteCommand>,
    registrations_tx: mpsc::UnboundedSender<(i32, oneshot::Sender<RemoteCommand>)>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
}

impl ConnectionSender {
    pub fn new(
        tx: mpsc::UnboundedSender<RemoteCommand>,
        registrations_tx: mpsc::UnboundedSender<(i32, oneshot::Sender<RemoteCommand>)>,
        receiver_shutdown: oneshot::Sender<()>,
    ) -> Self {
        Self {
            tx,
            registrations_tx,
            receiver_shutdown: Some(receiver_shutdown),
        }
    }

    pub async fn send(&self, cmd: RemoteCommand) -> Result<RemoteCommand, Error> {
        let (resolver, response) = oneshot::channel();
        let response = async {
            response
                .await
                .map_err(|_err| Error::Connection(ConnectionError::Disconnected))
        };
        match (
            self.registrations_tx.send((cmd.header.opaque, resolver)),
            self.tx.send(cmd),
        ) {
            (Ok(_), Ok(_)) => response.await,
            _ => Err(Error::Connection(ConnectionError::Disconnected)),
        }
    }
}

struct Receiver<S: Stream<Item = Result<RemoteCommand, Error>>> {
    inbound: Pin<Box<S>>,
    // internal sender
    outbound: mpsc::UnboundedSender<RemoteCommand>,
    pending_requests: HashMap<i32, oneshot::Sender<RemoteCommand>>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<(i32, oneshot::Sender<RemoteCommand>)>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
}

impl<S: Stream<Item = Result<RemoteCommand, Error>>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<RemoteCommand>,
        registrations: mpsc::UnboundedReceiver<(i32, oneshot::Sender<RemoteCommand>)>,
        shutdown: oneshot::Receiver<()>,
    ) -> Receiver<S> {
        Self {
            inbound: Box::pin(inbound),
            outbound,
            pending_requests: HashMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
        }
    }
}

impl<S: Stream<Item = Result<RemoteCommand, Error>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(ctx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(..)) => {
                return Poll::Ready(Err(()));
            }
            Poll::Pending => {}
        }
        loop {
            match self.registrations.as_mut().poll_next(ctx) {
                Poll::Ready(Some((opaque, resolver))) => {
                    self.pending_requests.insert(opaque, resolver);
                }
                Poll::Ready(None) => return Poll::Ready(Err(())),
                Poll::Pending => break,
            }
        }
        loop {
            match self.inbound.as_mut().poll_next(ctx) {
                Poll::Ready(Some(Ok(msg))) => {
                    if msg.is_response_type() {
                        if let Some(resolver) = self.pending_requests.remove(&msg.header.opaque) {
                            let _ = resolver.send(msg);
                        }
                    } else {
                        // FIXME: what to do?
                    }
                }
                Poll::Ready(None) => return Poll::Ready(Err(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(_e))) => return Poll::Ready(Err(())),
            }
        }
    }
}

pub struct Connection {
    addr: String,
    sender: ConnectionSender,
}

impl Connection {
    pub async fn new(addr: &str) -> Result<Self, Error> {
        let sender = Connection::prepare_stream(addr.to_string()).await?;
        Ok(Self {
            addr: addr.to_string(),
            sender,
        })
    }

    async fn prepare_stream(addr: String) -> Result<ConnectionSender, Error> {
        let stream = TcpStream::connect(&addr)
            .await
            .map(|stream| tokio_util::codec::Framed::new(stream, MqCodec))?;
        Connection::connect(stream).await
    }

    async fn connect<S>(stream: S) -> Result<ConnectionSender, Error>
    where
        S: Stream<Item = Result<RemoteCommand, Error>>,
        S: Sink<RemoteCommand, Error = Error>,
        S: Send + std::marker::Unpin + 'static,
    {
        let (mut sink, stream) = stream.split();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (registrations_tx, registrations_rx) = mpsc::unbounded_channel();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        tokio::spawn(Box::pin(Receiver::new(
            stream,
            tx.clone(),
            registrations_rx,
            receiver_shutdown_rx,
        )));
        tokio::spawn(Box::pin(async move {
            let mut opaque = 0;
            while let Some(mut msg) = rx.next().await {
                msg.header.opaque = opaque;
                opaque += 1;
                if let Err(_e) = sink.send(msg).await {
                    // FIXME: error handling
                    break;
                }
            }
        }));
        let sender = ConnectionSender::new(tx, registrations_tx, receiver_shutdown_tx);
        Ok(sender)
    }

    pub fn sender(&self) -> &ConnectionSender {
        &self.sender
    }
}