use crate::remoting::RemotingClient;

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
