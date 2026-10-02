//! HTTP listener setup, independent of market data and request admission.
use std::io;
use tokio::net::{TcpListener, TcpSocket, ToSocketAddrs, lookup_host};

/// Preserve hostname/address fallback while making the pending-connection limit explicit.
/// The kernel can cap this value, for example at `net.core.somaxconn` on Linux.
pub async fn bind(address: impl ToSocketAddrs, backlog: u32) -> io::Result<TcpListener> {
    if backlog == 0 || backlog > i32::MAX as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "listen backlog must be 1..2147483647",
        ));
    }
    let mut last_error = None;
    for address in lookup_host(address).await? {
        let result = (|| {
            let socket = if address.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            // Match Tokio/Mio's bind behavior; Windows has different reuse semantics.
            #[cfg(unix)]
            socket.set_reuseaddr(true)?;
            socket.bind(address)?;
            socket.listen(backlog)
        })();
        match result {
            Ok(listener) => return Ok(listener),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "no addresses resolved for listener",
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn falls_back_from_occupied_address_and_accepts_connection() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addresses = [
            occupied.local_addr().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        ];
        let listener = bind(&addresses[..], 1024).await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (accepted, peer) = listener.accept().await.unwrap();
        assert_eq!(peer, client.local_addr().unwrap());
        assert_eq!(
            accepted.local_addr().unwrap(),
            listener.local_addr().unwrap()
        );
    }
}
