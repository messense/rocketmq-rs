use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::net::SocketAddr;

use tokio::net::TcpStream;
use tokio::prelude::*;

use crate::protocol::RemoteCommand;
use crate::Error;

pub struct Config {
    name_server: String,
    client_ip: String,
    instance_name: String,
}

pub trait RemotingService {
    // fn invoke_async(&self, addr: &str, request: &RemoteCommand) -> Result<RemoteCommand, Error>;
    fn invoke_sync(&self, addr: &str, request: &RemoteCommand) -> Result<RemoteCommand, Error>;
    fn invoke_oneway(&self, addr: &str, request: &RemoteCommand) -> Result<(), Error>;
}

type ClientRequestFn = Box<fn(&RemoteCommand, &SocketAddr) -> Result<RemoteCommand, Error>>;

pub struct RemotingClient {
    processors: HashMap<i16, ClientRequestFn>,
    connection_table: HashMap<String, TcpStream>,
}

impl fmt::Debug for RemotingClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemotingClient")
            .field("connection_table", &self.connection_table)
            .finish()
    }
}

impl RemotingClient {
    pub fn new() -> Self {
        Self {
            processors: HashMap::new(),
            connection_table: HashMap::new(),
        }
    }

    pub fn register_request_fn(&mut self, code: i16, func: ClientRequestFn) {
        self.processors.insert(code, func);
    }

    async fn connect(&mut self, addr: &str) -> Result<&mut TcpStream, Error> {
        if let Some(conn) = self.connection_table.get_mut(addr) {
            // SAFETY: connection lives as long as RemotingClient
            // decouple the lifetimes to make the borrowck happy
            return Ok(unsafe { mem::transmute(conn) });
        }
        let stream = TcpStream::connect(addr).await.unwrap();
        self.connection_table.insert(addr.to_string(), stream);
        Ok(self.connection_table.get_mut(addr).unwrap())
    }

    async fn send_request(&self, conn: &mut TcpStream, cmd: &RemoteCommand) -> Result<(), Error> {
        use crate::protocol::JsonHeaderCodec;

        let content = cmd.encode(JsonHeaderCodec)?;
        conn.write_all(&content).await?;
        Ok(())
    }

    async fn receive_response(&self, conn: &mut TcpStream) -> Result<(), Error> {
        // FIXME: check connection closed and break out of loop
        loop {
            let length = conn.read_i32().await? as usize;
            let mut buf = vec![0; length];
            conn.read_exact(&mut buf).await?;
            let cmd = RemoteCommand::decode(&buf)?;
            self.process_command(cmd, conn).await?;
        }
    }

    async fn process_command(&self, cmd: RemoteCommand, conn: &mut TcpStream) -> Result<(), Error> {
        if cmd.is_response_type() {
        } else {
            if let Some(processor) = self.processors.get(&cmd.code()) {
                let mut res = processor(&cmd, &conn.peer_addr()?)?;
                res.header.opaque = cmd.header.opaque;
                res.header.flag |= 1 << 0;
                self.send_request(conn, &res).await?;
            }
        }
        Ok(())
    }
}

impl RemotingService for RemotingClient {
    fn invoke_sync(&self, addr: &str, request: &RemoteCommand) -> Result<RemoteCommand, Error> {
        todo!()
    }

    fn invoke_oneway(&self, addr: &str, request: &RemoteCommand) -> Result<(), Error> {
        todo!()
    }
}
