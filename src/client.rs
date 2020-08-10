use std::collections::HashMap;
use std::process;
use std::sync::{Arc, Mutex};

use if_addrs::get_if_addrs;

use crate::message::MessageQueue;
use crate::namesrv::NameServer;
use crate::producer::{PullResult, PullStatus};
use crate::protocol::{
    request::PullMessageRequestHeader, RemotingCommand, RequestCode, ResponseCode,
};
use crate::remoting::RemotingClient;
use crate::resolver::NsResolver;
use crate::Error;

pub trait InnerProducer {
    fn publish_topic_list(&self) -> Vec<String>;
    fn update_topic_publish_info(&self);
    fn is_publish_topic_need_update(&self, topic: &str) -> bool;
    fn is_unit_mode(&self) -> bool {
        false
    }
}

pub trait InnerConsumer {
    fn persist_consumer_offset(&self) -> Result<(), Error>;
    fn update_topic_subscribe_info(&self, topic: &str, mqs: &[MessageQueue]);
    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool;
    // fn subscription_data_list(&self);
    fn rebalance(&self);
    fn is_unit_mode(&self) -> bool {
        false
    }
    // fn get_consumer_running_info(&self);
    fn get_c_type(&self) -> String;
    fn get_model(&self) -> String;
    fn get_where(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct Credentials {
    access_key: String,
    secret_key: String,
    security_token: String,
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
            client_ip: client_ip_v4(),
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
            client_ip: client_ip_v4(),
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

fn client_ip_v4() -> String {
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
pub struct Client<P: InnerProducer, C: InnerConsumer, R: NsResolver> {
    options: ClientOptions,
    remote_client: RemotingClient,
    consumers: Arc<Mutex<HashMap<String, C>>>,
    producers: Arc<Mutex<HashMap<String, P>>>,
    name_server: NameServer<R>,
}

impl<P: InnerProducer, C: InnerConsumer, R: NsResolver> Client<P, C, R> {
    pub fn new(options: ClientOptions, name_server: NameServer<R>) -> Self {
        Self {
            options,
            remote_client: RemotingClient::new(),
            consumers: Arc::new(Mutex::new(HashMap::new())),
            producers: Arc::new(Mutex::new(HashMap::new())),
            name_server,
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

    pub fn register_consumer(&self, group: &str, consumer: C) {
        let mut consumers = self.consumers.lock().unwrap();
        consumers.entry(group.to_string()).or_insert(consumer);
    }

    pub fn unregister_consumer(&self, group: &str) {
        let mut consumers = self.consumers.lock().unwrap();
        consumers.remove(group);
    }

    pub fn register_producer(&self, group: &str, producer: P) {
        let mut producers = self.producers.lock().unwrap();
        producers.entry(group.to_string()).or_insert(producer);
    }

    pub fn unregister_producer(&self, group: &str) {
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
    use super::client_ip_v4;

    #[test]
    fn test_client_ip_v4() {
        let ip = client_ip_v4();
        assert_ne!(ip, "127.0.0.1");
    }
}
