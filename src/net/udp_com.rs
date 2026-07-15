use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

pub fn sync_sender_receiver_demo() -> std::io::Result<()> {
    // Socket öffnen: an alle Interfaces gebunden, Port 9999
    let socket = UdpSocket::bind("0.0.0.0:9999")?;

    // WICHTIG: Broadcast explizit erlauben (Linux-Default ist off)
    socket.set_broadcast(true)?;

    // Empfangs-Timeout (sonst blockt recv_from ewig)
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;

    // ── Senden ──
    let broadcast_addr: SocketAddr = "192.168.100.255:9999".parse().unwrap();
    let payload = b"hello from node A, cycle=42";
    socket.send_to(payload, broadcast_addr)?;
    println!("Gesendet: {} bytes an {}", payload.len(), broadcast_addr);

    // ── Empfangen ──
    let mut buf = [0u8; 1500]; // MTU-groß
    match socket.recv_from(&mut buf) {
        Ok((len, src)) => {
            println!("Empfangen: {} bytes von {}", len, src);
            println!("Daten: {:?}", &buf[..len]);
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            println!("Timeout, nichts empfangen");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
