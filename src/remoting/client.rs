use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::connection::Connection;
use crate::client::Credentials;
use crate::error::{ConnectionError, Error};
use crate::protocol::RemotingCommand;

type HmacSha1 = Hmac<sha1::Sha1>;

enum ConnectionStatus {
    Connected(Arc<Connection>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection>, Error>>>),
}

#[derive(Clone)]
pub struct RemotingClient {
    connections: Arc<Mutex<HashMap<String, ConnectionStatus>>>,
    credentials: Option<Credentials>,
}

impl fmt::Debug for RemotingClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemotingClient")
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl Default for RemotingClient {
    fn default() -> Self {
        Self::new(None)
    }
}

impl RemotingClient {
    pub fn new<C: Into<Option<Credentials>>>(credentials: C) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            credentials: credentials.into(),
        }
    }

    pub async fn invoke(&self, addr: &str, cmd: RemotingCommand) -> Result<RemotingCommand, Error> {
        let conn = self.get_connection(addr).await?;
        let sender = conn.sender();
        Ok(sender.send(self.add_signature(cmd)).await?)
    }

    pub async fn invoke_oneway(&self, addr: &str, cmd: RemotingCommand) -> Result<(), Error> {
        let conn = self.get_connection(addr).await?;
        let sender = conn.sender();
        Ok(sender.send_oneway(self.add_signature(cmd)).await?)
    }

    pub async fn get_connection(&self, addr: &str) -> Result<Arc<Connection>, Error> {
        let rx = {
            match self.connections.lock().get_mut(addr) {
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

    pub fn shutdown(&self) {
        let mut connections = self.connections.lock();
        connections.clear();
    }

    async fn connect(&self, addr: &str) -> Result<Arc<Connection>, Error> {
        let rx = {
            match self
                .connections
                .lock()
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
        let old = self.connections.lock().insert(
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

    fn add_signature(&self, mut cmd: RemotingCommand) -> RemotingCommand {
        if let Some(credentials) = &self.credentials {
            let size = cmd.header.ext_fields.len() + 1;
            let mut m = HashMap::with_capacity(size);
            m.insert("AccessKey".to_string(), &credentials.access_key);
            let mut order = Vec::with_capacity(size);
            order.push("AccessKey");
            if let Some(security_token) = &credentials.security_token {
                if !security_token.is_empty() {
                    m.insert("SecurityToken".to_string(), security_token);
                    cmd.header
                        .ext_fields
                        .insert("SecurityToken".to_string(), security_token.clone());
                }
            }
            for (k, v) in &cmd.header.ext_fields {
                m.insert(k.clone(), v);
                order.push(&k[..]);
            }
            order.sort();
            let mut content = Vec::with_capacity(cmd.body.len());
            for key in order {
                content.extend_from_slice(&m[key].as_bytes());
            }
            content.extend_from_slice(&cmd.body);
            cmd.header.ext_fields.insert(
                "Signature".to_string(),
                Self::calculate_signature(&content, credentials.secret_key.as_bytes()),
            );
            cmd.header
                .ext_fields
                .insert("AccessKey".to_string(), credentials.access_key.clone());
        }
        cmd
    }

    fn calculate_signature(data: &[u8], key: &[u8]) -> String {
        let mut mac = HmacSha1::new_from_slice(key).unwrap();
        mac.update(data);
        let result = mac.finalize().into_bytes();
        base64::encode(&result)
    }
}

#[cfg(test)]
mod test {
    use super::RemotingClient;

    #[test]
    fn test_calculate_signature() {
        let signature = RemotingClient::calculate_signature(
            b"Hello RocketMQ Client ACL Feature",
            b"adiaushdiaushd",
        );
        assert_eq!(signature, "tAb/54Rwwcq+pbH8Loi7FWX4QSQ=");
    }
}
