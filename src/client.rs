use std::collections::HashMap;
use std::process;
use std::sync::{Arc, Mutex, Once};

use if_addrs::get_if_addrs;
use tokio::sync::oneshot;
use tokio::time;

use crate::consumer::ConsumerInner;
use crate::message::MessageQueue;
use crate::namesrv::NameServer;
use crate::producer::{ProducerInner, PullResult, PullStatus};
use crate::protocol::{
    request::PullMessageRequestHeader, RemotingCommand, RequestCode, ResponseCode,
};
use crate::remoting::RemotingClient;
use crate::resolver::NsResolver;
use crate::Error;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub security_token: Option<String>,
}

impl Credentials {
    pub fn new<S: Into<String>>(access_key: S, secret_key: S) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            security_token: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    group_name: String,
    name_server_addrs: Vec<String>,
    // namesrv
    client_ip: String,
    instance_name: String,
    unit_mode: bool,
    unit_name: String,
    vip_channel_enabled: bool,
    retry_times: usize,
    credentials: Option<Credentials>,
    namespace: String,
    // resolver
}

impl ClientOptions {
    pub fn new(group: &str) -> Self {
        Self {
            group_name: group.to_string(),
            name_server_addrs: Vec::new(),
            client_ip: client_ipv4(),
            instance_name: "DEFAULT".to_string(),
            unit_mode: false,
            unit_name: String::new(),
            vip_channel_enabled: false,
            retry_times: 3,
            credentials: None,
            namespace: String::new(),
        }
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            group_name: String::new(),
            name_server_addrs: Vec::new(),
            client_ip: client_ipv4(),
            instance_name: "DEFAULT".to_string(),
            unit_mode: false,
            unit_name: String::new(),
            vip_channel_enabled: false,
            retry_times: 3,
            credentials: None,
            namespace: String::new(),
        }
    }
}

fn client_ipv4() -> String {
    if let Ok(addrs) = get_if_addrs() {
        for addr in addrs {
            if addr.is_loopback() {
                continue;
            }
            let ip = addr.ip();
            if ip.is_ipv4() {
                return ip.to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

#[derive(Debug)]
pub struct Client<R: NsResolver + Clone> {
    options: ClientOptions,
    remote_client: RemotingClient,
    consumers: Arc<Mutex<HashMap<String, Arc<ConsumerInner>>>>,
    producers: Arc<Mutex<HashMap<String, Arc<ProducerInner>>>>,
    name_server: NameServer<R>,
    once: Once,
    shutdown_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
}

impl<R> Client<R>
where
    R: NsResolver + Clone + Send + Sync + 'static,
{
    pub fn new(options: ClientOptions, name_server: NameServer<R>) -> Self {
        let credentials = options.credentials.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        Self {
            options,
            remote_client: RemotingClient::new(credentials),
            consumers: Arc::new(Mutex::new(HashMap::new())),
            producers: Arc::new(Mutex::new(HashMap::new())),
            name_server,
            once: Once::new(),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Get Client ID
    pub fn id(&self) -> String {
        let mut client_id = self.options.client_ip.clone() + "@";
        if self.options.instance_name == "DEFAULT" {
            client_id.push_str(&process::id().to_string());
        } else {
            client_id.push_str(&self.options.instance_name);
        }
        if !self.options.unit_name.is_empty() {
            client_id.push_str(&self.options.unit_name);
        }
        client_id
    }

    pub fn start(&self) {
        self.once.call_once(|| {
            // Update name server address
            let name_server = self.name_server.clone();
            tokio::spawn(async move {
                time::delay_for(time::Duration::from_secs(10)).await;
                let mut interval = time::interval(time::Duration::from_secs(2 * 60));
                loop {
                    interval.tick().await;
                    let _res = name_server.update_name_server_address();
                }
            });

            // Update route info

            // Send heartbeat to brokers

            // Persist offset

            // Rebalance
        })
    }

    pub fn shutdown(&self) {}

    #[inline]
    pub async fn invoke(&self, addr: &str, cmd: RemotingCommand) -> Result<RemotingCommand, Error> {
        Ok(self.remote_client.invoke(addr, cmd).await?)
    }

    #[inline]
    pub async fn invoke_oneway(&self, addr: &str, cmd: RemotingCommand) -> Result<(), Error> {
        Ok(self.remote_client.invoke_oneway(addr, cmd).await?)
    }

    pub async fn pull_message(
        &self,
        addr: &str,
        request: PullMessageRequestHeader,
    ) -> Result<PullResult, Error> {
        let cmd = RemotingCommand::with_header(RequestCode::PullMessage, request, Vec::new());
        let res = self.remote_client.invoke(addr, cmd).await?;
        let status = match ResponseCode::from_code(res.code())? {
            ResponseCode::Success => PullStatus::Found,
            ResponseCode::PullNotFound => PullStatus::NoNewMsg,
            ResponseCode::PullRetryImmediately => PullStatus::NoMsgMatched,
            ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
            _ => {
                return Err(Error::ResponseError {
                    code: res.code(),
                    message: format!(
                        "unknown response code: {}, remark: {}",
                        res.code(),
                        res.header.remark
                    ),
                });
            }
        };
        let ext_fields = &res.header.ext_fields;
        let max_offset = ext_fields
            .get("maxOffset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        let min_offset = ext_fields
            .get("minOffset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        let next_begin_offset = ext_fields
            .get("nextBeginOffset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        let suggest_which_broker_id = ext_fields
            .get("suggestWhichBrokerId")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_default();
        Ok(PullResult {
            next_begin_offset,
            min_offset,
            max_offset,
            suggest_which_broker_id,
            status,
            message_exts: Vec::new(),
            body: res.body,
        })
    }

    pub(crate) fn register_consumer(&self, group: &str, consumer: Arc<ConsumerInner>) {
        let mut consumers = self.consumers.lock().unwrap();
        consumers.entry(group.to_string()).or_insert(consumer);
    }

    pub(crate) fn unregister_consumer(&self, group: &str) {
        let mut consumers = self.consumers.lock().unwrap();
        consumers.remove(group);
    }

    pub(crate) fn register_producer(&self, group: &str, producer: Arc<ProducerInner>) {
        let mut producers = self.producers.lock().unwrap();
        producers.entry(group.to_string()).or_insert(producer);
    }

    pub(crate) fn unregister_producer(&self, group: &str) {
        let mut producers = self.producers.lock().unwrap();
        producers.remove(group);
    }

    fn rebalance_imediately(&self) {
        let consumers = self.consumers.lock().unwrap();
        for consumer in consumers.values() {
            consumer.rebalance();
        }
    }
}

#[cfg(test)]
mod test {
    use super::client_ipv4;

    #[test]
    fn test_client_ip_v4() {
        let ip = client_ipv4();
        assert_ne!(ip, "127.0.0.1");
    }
}
