use std::sync::Mutex;

use crate::nsresolver::NsResolver;
use crate::remoting::RemotingClient;
use crate::Error;

#[derive(Debug)]
struct NameServersInner {
    servers: Vec<String>,
    index: usize,
}

#[derive(Debug)]
pub struct NameServers<NR: NsResolver> {
    inner: Mutex<NameServersInner>,
    resolver: NR,
    remoting_client: RemotingClient,
}

impl<NR: NsResolver> NameServers<NR> {
    pub fn new(resolver: NR) -> Result<Self, Error> {
        let servers = resolver.resolve()?;
        let inner = NameServersInner { servers, index: 0 };
        // TODO: check addrs
        Ok(Self {
            inner: Mutex::new(inner),
            resolver,
            remoting_client: RemotingClient::new(),
        })
    }

    pub fn get_address(&self) -> String {
        let mut inner = self.inner.lock().unwrap();
        let addr = &inner.servers[inner.index].clone();
        let mut index = inner.index + 1;
        index %= inner.servers.len();
        inner.index = index;
        addr.trim_start_matches("http(s)://").to_string()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().servers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().servers.is_empty()
    }

    pub fn update(&mut self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        if let Ok(servers) = self.resolver.resolve() {
            inner.servers = servers;
        }
        Ok(())
    }
}
