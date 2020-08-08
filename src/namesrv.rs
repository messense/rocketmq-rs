use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Mutex;

use rand::prelude::*;

use crate::message::MessageQueue;
use crate::nsresolver::NsResolver;
use crate::permission::Permission;
use crate::protocol::{
    request::{GetRouteInfoRequestHeader, RequestCode},
    response::ResponseCode,
    RemotingCommand,
};
use crate::remoting::RemotingClient;
use crate::route::{BrokerData, TopicRouteData, MASTER_ID};
use crate::Error;

#[derive(Debug)]
struct NameServerInner {
    servers: Vec<String>,
    index: usize,
    // broker name -> BrokerData
    broker_address_map: HashMap<String, BrokerData>,
    // topic name -> TopicRouteData
    route_data_map: HashMap<String, TopicRouteData>,
}

#[derive(Debug)]
pub struct NameServer<NR: NsResolver> {
    inner: Mutex<NameServerInner>,
    resolver: NR,
    remoting_client: RemotingClient,
}

impl<NR: NsResolver> NameServer<NR> {
    pub fn new(resolver: NR) -> Result<Self, Error> {
        let servers = resolver.resolve()?;
        let inner = NameServerInner {
            servers,
            index: 0,
            broker_address_map: HashMap::new(),
            route_data_map: HashMap::new(),
        };
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

    pub fn update_name_server_address(&mut self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        if let Ok(servers) = self.resolver.resolve() {
            inner.servers = servers;
        }
        Ok(())
    }

    pub async fn query_topic_route_info(&self, topic: &str) -> Result<TopicRouteData, Error> {
        let inner = self.inner.lock().unwrap();
        if inner.servers.is_empty() {
            return Err(Error::EmptyNameServers);
        }
        let header = GetRouteInfoRequestHeader {
            topic: topic.to_string(),
        };
        for addr in &inner.servers {
            let cmd = RemotingCommand::with_header(
                RequestCode::GetRouteInfoByTopic.into(),
                header.clone(),
                Vec::new(),
            );
            let res = self.remoting_client.invoke(addr, cmd).await;
            if let Ok(res) = res {
                match ResponseCode::try_from(res.header.code).unwrap() {
                    ResponseCode::Success => {
                        if res.body.is_empty() {
                            // FIXME: error
                        }
                        let route_data = TopicRouteData::from_bytes(&res.body)?;
                        return Ok(route_data);
                    }
                    ResponseCode::TopicNotExist => {
                        return Err(Error::TopicNotExist(topic.to_string()))
                    }
                    _ => {
                        return Err(Error::ResponseError {
                            code: res.header.code,
                            message: res.header.remark.clone(),
                        })
                    }
                }
            } else {
                println!("{:?}", res);
            }
        }
        Err(Error::EmptyRouteData)
    }

    pub async fn update_topic_route_info(&self, topic: &str) -> Result<bool, Error> {
        Ok(self
            .update_topic_route_info_with_default(topic, "", 0)
            .await?)
    }

    pub async fn update_topic_route_info_with_default(
        &self,
        topic: &str,
        default_topic: &str,
        default_queue_num: i32,
    ) -> Result<bool, Error> {
        let t = if !default_topic.is_empty() {
            default_topic
        } else {
            topic
        };
        let mut route_data = self.query_topic_route_info(t).await?;
        if !default_topic.is_empty() {
            for queue in &mut route_data.queue_datas {
                if queue.read_queue_nums > default_queue_num {
                    queue.read_queue_nums = default_queue_num;
                    queue.write_queue_nums = default_queue_num;
                }
            }
        }
        let mut inner = self.inner.lock().unwrap();
        let changed = inner
            .route_data_map
            .get(topic)
            .map(|old_route_data| Self::is_topic_route_data_changed(old_route_data, &route_data))
            .unwrap_or(true);
        if changed {
            for broker_data in &route_data.broker_datas {
                inner
                    .broker_address_map
                    .insert(broker_data.broker_name.clone(), broker_data.clone());
            }
            inner.route_data_map.insert(topic.to_string(), route_data);
        }
        Ok(changed)
    }

    fn is_topic_route_data_changed(old_data: &TopicRouteData, new_data: &TopicRouteData) -> bool {
        let mut old_data = old_data.clone();
        let mut new_data = new_data.clone();
        old_data.queue_datas.sort_by_key(|k| k.broker_name.clone());
        old_data.broker_datas.sort_by_key(|k| k.broker_name.clone());
        new_data.queue_datas.sort_by_key(|k| k.broker_name.clone());
        new_data.broker_datas.sort_by_key(|k| k.broker_name.clone());
        old_data != new_data
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        topic: &str,
    ) -> Result<Vec<MessageQueue>, Error> {
        let route_data = self.query_topic_route_info(topic).await?;
        let mqs: Vec<MessageQueue> = route_data
            .queue_datas
            .into_iter()
            .flat_map(|queue_data| {
                let mut mqs = Vec::new();
                if let Some(perm) = Permission::from_bits(queue_data.perm) {
                    if perm.is_readable() {
                        for i in 0..queue_data.read_queue_nums {
                            mqs.push(MessageQueue {
                                topic: topic.to_string(),
                                broker_name: queue_data.broker_name.clone(),
                                queue_id: i as u32,
                            })
                        }
                    }
                }
                mqs
            })
            .collect();
        Ok(mqs)
    }

    pub async fn fetch_publish_message_queues(
        &self,
        topic: &str,
    ) -> Result<Vec<MessageQueue>, Error> {
        let inner = self.inner.lock().unwrap();
        if let Some(route_data) = inner.route_data_map.get(topic) {
            let publish_info = route_data.to_publish_info(topic);
            Ok(publish_info.message_queues)
        } else {
            // Avoid deadlock
            drop(inner);
            let route_data = self.query_topic_route_info(topic).await?;
            let mut inner = self.inner.lock().unwrap();
            inner
                .route_data_map
                .insert(topic.to_string(), route_data.clone());
            // Add brokers
            for broker_data in &route_data.broker_datas {
                inner
                    .broker_address_map
                    .insert(broker_data.broker_name.to_string(), broker_data.clone());
            }
            let publish_info = route_data.to_publish_info(topic);
            Ok(publish_info.message_queues)
        }
    }

    pub fn find_broker_addr_by_topic(&self, topic: &str) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        if let Some(route_data) = inner.route_data_map.get(topic) {
            if route_data.broker_datas.is_empty() {
                return None;
            }
            let mut rng = thread_rng();
            let broker_data = route_data.broker_datas.iter().choose(&mut rng).unwrap();
            if let Some(addr) = broker_data.broker_addrs.get(&MASTER_ID) {
                if addr.is_empty() {
                    if let Some(addr) = broker_data.broker_addrs.values().choose(&mut rng) {
                        return Some(addr.to_string());
                    }
                } else {
                    return Some(addr.to_string());
                }
            }
        }
        None
    }

    pub fn find_broker_addr_by_name(&self, broker_name: &str) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        inner
            .broker_address_map
            .get(broker_name)
            .and_then(|broker_data| broker_data.broker_addrs.get(&MASTER_ID).cloned())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::nsresolver::StaticResolver;

    #[tokio::test]
    async fn test_query_topic_route_info_with_empty_namesrv() {
        let namesrv = NameServer::new(StaticResolver::new(vec![])).unwrap();
        let res = namesrv.query_topic_route_info("test").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_query_topic_route_info() {
        let namesrv =
            NameServer::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        let res = namesrv.query_topic_route_info("TopicTest").await;
        println!("{:?}", res);
        assert!(!res.is_err());
    }

    #[tokio::test]
    async fn test_update_topic_route_info() {
        let namesrv =
            NameServer::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        assert!(namesrv.update_topic_route_info("TopicTest").await.unwrap());
        assert!(!namesrv.update_topic_route_info("TopicTest").await.unwrap());
    }

    #[tokio::test]
    async fn test_fetch_subscribe_message_queues() {
        let namesrv =
            NameServer::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        let res = namesrv
            .fetch_subscribe_message_queues("TopicTest")
            .await
            .unwrap();
        assert!(!res.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_publish_message_queues() {
        let namesrv =
            NameServer::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        let res = namesrv
            .fetch_publish_message_queues("TopicTest")
            .await
            .unwrap();
        assert!(!res.is_empty());
    }

    #[tokio::test]
    pub async fn find_broker_addr_by_topic() {
        let namesrv =
            NameServer::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        namesrv.update_topic_route_info("TopicTest").await.unwrap();
        let addr = namesrv.find_broker_addr_by_topic("TopicTest").unwrap();
        assert!(addr.ends_with(":10911"));
    }

    #[tokio::test]
    pub async fn find_broker_addr_by_name() {
        let namesrv =
            NameServer::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        namesrv.update_topic_route_info("TopicTest").await.unwrap();
        let res = namesrv.query_topic_route_info("TopicTest").await.unwrap();
        let broker_name = res.broker_datas.first().map(|x| &x.broker_name).unwrap();
        let addr = namesrv.find_broker_addr_by_name(broker_name).unwrap();
        assert!(addr.ends_with(":10911"));
    }
}
