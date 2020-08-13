use std::net::IpAddr;

use if_addrs::get_if_addrs;

pub fn client_ip_addr() -> Option<IpAddr> {
    let mut ipv6_addrs = Vec::new();
    if let Ok(addrs) = get_if_addrs() {
        for addr in addrs {
            if addr.is_loopback() {
                continue;
            }
            let ip = addr.ip();
            if ip.is_ipv4() {
                let ip_str = ip.to_string();
                if ip_str.starts_with("127.0") || ip_str.starts_with("192.") {
                    continue;
                } else {
                    return Some(ip);
                }
            } else {
                ipv6_addrs.push(ip);
            }
        }
    }
    // did not find ipv4 address, try ipv6
    ipv6_addrs.first().cloned()
}

#[cfg(test)]
mod test {
    use super::client_ip_addr;

    #[test]
    fn test_client_ip_addr() {
        let ip = client_ip_addr();
        assert!(ip.is_some());
    }
}
