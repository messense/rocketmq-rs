use std::process;

use crate::producer::{PullResult, PullStatus};
use crate::protocol::{
    request::PullMessageRequestHeader, RemotingCommand, RequestCode, ResponseCode,
};
use crate::remoting::RemotingClient;
use crate::Error;

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

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            group_name: String::new(),
            name_server_addrs: Vec::new(),
            client_ip: String::new(), // FIXME
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

#[derive(Debug)]
pub struct Client {
    options: ClientOptions,
    remote_client: RemotingClient,
}

impl Client {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            options,
            remote_client: RemotingClient::new(),
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
                })
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
}
