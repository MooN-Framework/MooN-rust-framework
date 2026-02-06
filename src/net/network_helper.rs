use if_addrs::{IfAddr, get_if_addrs};
use std::net::Ipv4Addr;

pub fn get_eth0_ipv4_and_broadcast() -> Option<(Ipv4Addr, Ipv4Addr)> {
    if let Ok(ifaces) = get_if_addrs() {
        for iface in ifaces {
            if iface.name == "eth0"
                && let IfAddr::V4(ifv4) = iface.addr
            {
                let ip = u32::from(ifv4.ip);
                let netmask = u32::from(ifv4.netmask);
                let broadcast = Ipv4Addr::from(ip | !netmask);
                return Some((ifv4.ip, broadcast));
            }
        }
    }
    None
}
