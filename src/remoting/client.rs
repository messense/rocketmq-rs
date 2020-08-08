use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use super::connection::Connection;
use crate::error::{ConnectionError, Error};
use crate::protocol::RemotingCommand;

enum ConnectionStatus {
    Connected(Arc<Connection>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection>, Error>>>),
}

#[derive(Clone)]
pub struct RemotingClient {
    connections: Arc<Mutex<HashMap<String, ConnectionStatus>>>,
}

impl fmt::Debug for RemotingClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemotingClient").finish()
    }
}

impl RemotingClient {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn invoke(&self, addr: &str, cmd: RemotingCommand) -> Result<RemotingCommand, Error> {
        let conn = self.get_connection(addr).await?;
        let sender = conn.sender();
        Ok(sender.send(cmd).await?)
    }

    pub async fn get_connection(&self, addr: &str) -> Result<Arc<Connection>, Error> {
        let rx = {
            match self.connections.lock().unwrap().get_mut(addr) {
                None => None,
                Some(ConnectionStatus::Connected(conn)) => return Ok(conn.clone()),
                Some(ConnectionStatus::Connecting(ref mut v)) => {
                    let (tx, rx) = oneshot::channel();
                    v.push(tx);
                    Some(rx)
                }
            }
        };
        match rx {
            None => self.connect(addr).await,
            Some(rx) => match rx.await {
                Ok(res) => res,
                Err(_) => Err(Error::Connection(ConnectionError::Canceled)),
            },
        }
    }

    async fn connect(&self, addr: &str) -> Result<Arc<Connection>, Error> {
        let rx = {
            match self
                .connections
                .lock()
                .unwrap()
                .entry(addr.to_string())
                .or_insert_with(|| ConnectionStatus::Connecting(Vec::new()))
            {
                ConnectionStatus::Connecting(ref mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        let (tx, rx) = oneshot::channel();
                        v.push(tx);
                        Some(rx)
                    }
                }
                ConnectionStatus::Connected(_) => None,
            }
        };
        if let Some(rx) = rx {
            return match rx.await {
                Ok(res) => res,
                Err(_) => Err(Error::Connection(ConnectionError::Canceled)),
            };
        }
        // FIXME: connection backoff
        let conn = Connection::new(addr).await?;
        let c = Arc::new(conn);
        let old = self.connections.lock().unwrap().insert(
            addr.to_string(),
            ConnectionStatus::Connected(Arc::clone(&c)),
        );
        match old {
            Some(ConnectionStatus::Connecting(mut v)) => {
                for tx in v.drain(..) {
                    let _ = tx.send(Ok(c.clone()));
                }
            }
            Some(ConnectionStatus::Connected(_)) => {}
            None => {}
        }
        Ok(c)
    }
}
