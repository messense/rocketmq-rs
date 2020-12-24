use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};

use futures::{
    task::{Context, Poll},
    Future, Sink, SinkExt, Stream, StreamExt,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::error::{ConnectionError, Error};
use crate::protocol::{MqCodec, RemotingCommand};

pub struct ConnectionSender {
    addr: String,
    tx: mpsc::UnboundedSender<RemotingCommand>,
    registrations_tx: mpsc::UnboundedSender<(i32, oneshot::Sender<RemotingCommand>)>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    opaque_id: AtomicI32,
}

impl fmt::Debug for ConnectionSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSender")
            .field("addr", &self.addr)
            .finish()
    }
}

impl ConnectionSender {
    pub fn new(
        addr: String,
        tx: mpsc::UnboundedSender<RemotingCommand>,
        registrations_tx: mpsc::UnboundedSender<(i32, oneshot::Sender<RemotingCommand>)>,
        receiver_shutdown: oneshot::Sender<()>,
    ) -> Self {
        Self {
            addr,
            tx,
            registrations_tx,
            receiver_shutdown: Some(receiver_shutdown),
            opaque_id: AtomicI32::new(1),
        }
    }

    #[tracing::instrument(skip(self, cmd))]
    pub async fn send(&self, cmd: RemotingCommand) -> Result<RemotingCommand, Error> {
        let (sender, receiver) = oneshot::channel();
        let mut cmd = cmd;
        cmd.header.opaque = self.opaque_id.fetch_add(1, Ordering::SeqCst);
        debug!(
            code = cmd.code(),
            opaque = cmd.header.opaque,
            cmd = ?cmd,
            "sending remoting command to {}",
            &self.addr
        );
        match (
            self.registrations_tx.send((cmd.header.opaque, sender)),
            self.tx.send(cmd),
        ) {
            (Ok(_), Ok(_)) => receiver
                .await
                .map_err(|_err| Error::Connection(ConnectionError::Disconnected)),
            _ => Err(Error::Connection(ConnectionError::Disconnected)),
        }
    }

    pub async fn send_oneway(&self, cmd: RemotingCommand) -> Result<(), Error> {
        let mut cmd = cmd;
        cmd.header.opaque = self.opaque_id.fetch_add(1, Ordering::SeqCst);
        self.tx
            .send(cmd)
            .map_err(|_| Error::Connection(ConnectionError::Disconnected))?;
        Ok(())
    }
}

struct Receiver<S: Stream<Item = Result<RemotingCommand, Error>>> {
    addr: String,
    inbound: Pin<Box<S>>,
    // internal sender
    outbound: mpsc::UnboundedSender<RemotingCommand>,
    pending_requests: HashMap<i32, oneshot::Sender<RemotingCommand>>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<(i32, oneshot::Sender<RemotingCommand>)>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
}

impl<S: Stream<Item = Result<RemotingCommand, Error>>> Receiver<S> {
    pub fn new(
        addr: String,
        inbound: S,
        outbound: mpsc::UnboundedSender<RemotingCommand>,
        registrations: mpsc::UnboundedReceiver<(i32, oneshot::Sender<RemotingCommand>)>,
        shutdown: oneshot::Receiver<()>,
    ) -> Receiver<S> {
        Self {
            addr,
            inbound: Box::pin(inbound),
            outbound,
            pending_requests: HashMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
        }
    }
}

impl<S: Stream<Item = Result<RemotingCommand, Error>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(ctx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(..)) => {
                return Poll::Ready(Err(()));
            }
            Poll::Pending => {}
        }
        loop {
            match self.registrations.as_mut().poll_recv(ctx) {
                Poll::Ready(Some((opaque, resolver))) => {
                    self.pending_requests.insert(opaque, resolver);
                }
                Poll::Ready(None) => return Poll::Ready(Err(())),
                Poll::Pending => break,
            }
        }
        #[allow(clippy::never_loop)]
        loop {
            match self.inbound.as_mut().poll_next(ctx) {
                Poll::Ready(Some(Ok(msg))) => {
                    debug!(
                        code = msg.code(),
                        opaque = msg.header.opaque,
                        remark = %msg.header.remark,
                        cmd = ?msg,
                        "received remoting command from {}",
                        &self.addr
                    );
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

    #[tracing::instrument(name = "connect")]
    async fn prepare_stream(addr: String) -> Result<ConnectionSender, Error> {
        info!("connecting to server");
        let stream = TcpStream::connect(&addr)
            .await
            .map(|stream| tokio_util::codec::Framed::new(stream, MqCodec))?;
        info!("server connected");
        Connection::connect(addr, stream).await
    }

    async fn connect<S>(addr: String, stream: S) -> Result<ConnectionSender, Error>
    where
        S: Stream<Item = Result<RemotingCommand, Error>>,
        S: Sink<RemotingCommand, Error = Error>,
        S: Send + std::marker::Unpin + 'static,
    {
        let (mut sink, stream) = stream.split();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (registrations_tx, registrations_rx) = mpsc::unbounded_channel();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        tokio::spawn(Box::pin(Receiver::new(
            addr.clone(),
            stream,
            tx.clone(),
            registrations_rx,
            receiver_shutdown_rx,
        )));
        tokio::spawn(Box::pin(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(_e) = sink.send(msg).await {
                    // FIXME: error handling
                    break;
                }
            }
        }));
        let sender = ConnectionSender::new(addr, tx, registrations_tx, receiver_shutdown_tx);
        Ok(sender)
    }

    pub fn sender(&self) -> &ConnectionSender {
        &self.sender
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            info!("shutting down connection to {}", &self.addr);
            let _ = shutdown.send(());
        }
    }
}
