use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Mutex;

use crate::nsresolver::NsResolver;
use crate::protocol::{
    request::{GetRouteInfoRequestHeader, RequestCode},
    response::ResponseCode,
    RemotingCommand,
};
use crate::remoting::RemotingClient;
use crate::route::{BrokerData, TopicRouteData};
use crate::Error;

#[derive(Debug)]
struct NameServersInner {
    servers: Vec<String>,
    index: usize,
    // broker name -> BrokerData
    broker_address_map: HashMap<String, BrokerData>,
    // topic name -> TopicRouteData
    route_data_map: HashMap<String, TopicRouteData>,
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
        let inner = NameServersInner {
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

    pub fn update(&mut self) -> Result<(), Error> {
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::nsresolver::StaticResolver;

    #[tokio::test]
    async fn test_query_topic_route_info_with_empty_namesrv() {
        let namesrv = NameServers::new(StaticResolver::new(vec![])).unwrap();
        let res = namesrv.query_topic_route_info("test").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_query_topic_route_info() {
        let namesrv =
            NameServers::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        let res = namesrv.query_topic_route_info("TopicTest").await;
        println!("{:?}", res);
        assert!(!res.is_err());
    }

    #[tokio::test]
    async fn test_update_topic_route_info() {
        let namesrv =
            NameServers::new(StaticResolver::new(vec!["localhost:9876".to_string()])).unwrap();
        assert!(namesrv.update_topic_route_info("TopicTest").await.unwrap());
        assert!(!namesrv.update_topic_route_info("TopicTest").await.unwrap());
    }
}
