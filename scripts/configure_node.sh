sudo cat /run/NetworkManager/system-connections/netplan-eth0.nmconnection

[connection]
id=netplan-eth0
type=ethernet
uuid=75a1216a-9d1a-30cd-8aca-ace5526ec021

[ethernet]
wake-on-lan=0

[ipv4]
method=manual
address1=192.168.1.3/24
gateway=192.168.1.1
dns=192.168.1.1;1.1.1.1;

[ipv6]
method=auto
ip6-privacy=0

[proxy]