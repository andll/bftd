use crate::block::ValidatorIndex;
use bytes::Bytes;
use futures::future::join_all;
use futures::join;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use snow::params::NoiseParams;
use snow::{HandshakeState, TransportState};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::{io, select};

const MAX_MESSAGE: usize = 1024;
const INIT_PAYLOAD: [u8; 4] = [10, 15, 32, 5];

// todo
// timeout
// measure ping
// spawn for handshake
// improve task tracking
pub struct ConnectionPool {
    acceptor_task: JoinHandle<io::Result<()>>,
    connections: mpsc::Receiver<Connection>,
    connection_tasks: Vec<JoinHandle<()>>,
    known_peers: Vec<PeerInfo>,
}

#[derive(Clone)] // todo - zeroize
pub struct NoisePrivateKey(Arc<Box<[u8]>>);

#[derive(Clone)]
pub struct PeerInfo {
    pub address: SocketAddr,
    pub public_key: NoisePublicKey,
    pub index: ValidatorIndex,
}

#[derive(Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Default)]
pub struct NoisePublicKey(pub Vec<u8>);

pub struct NetworkMessage {
    pub data: Bytes,
}

pub struct Connection {
    pub sender: mpsc::Sender<NetworkMessage>,
    pub receiver: mpsc::Receiver<NetworkMessage>,
    pub peer: PeerInfo,
}

struct Acceptor {
    listener: TcpListener,
    pk: NoisePrivateKey,
    connection_senders: HashMap<NoisePublicKey, mpsc::Sender<NoiseConnection>>,
}

struct NoiseConnection {
    writer: FrameWriter,
    reader: FrameReader,
    #[allow(dead_code)]
    addr: SocketAddr,
}

struct Handshake {
    pk: NoisePrivateKey,
    socket: TcpStream,
}

struct FrameReader {
    reader: OwnedReadHalf,
    net_buffer: Box<[u8]>,
    noise_buffer: Box<[u8]>,
    transport_state: Option<Arc<Mutex<TransportState>>>,
}

struct FrameWriter {
    writer: OwnedWriteHalf,
    buffer: Box<[u8]>,
    transport_state: Option<Arc<Mutex<TransportState>>>,
}

impl ConnectionPool {
    pub async fn start<A: ToSocketAddrs>(
        addr: A,
        pk: NoisePrivateKey,
        peers: Vec<PeerInfo>,
        self_index: usize,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let mut connection_senders = HashMap::new();
        let mut connection_tasks = Vec::with_capacity(peers.len());
        let (connections_sender, connections) = mpsc::channel(100);
        for (peer_index, peer) in peers.iter().enumerate() {
            assert_eq!(
                peer_index as u64, peer.index.0,
                "Peer index does not match peer position"
            );
            if peer_index == self_index {
                continue;
            }
            let active = peer_index > self_index;
            let (s, r) = mpsc::channel(10);
            connection_senders.insert(peer.public_key.clone(), s);
            let connection_task = PeerTask {
                incoming: r,
                peer: peer.clone(),
                pk: pk.clone(),
                active,
                connections_sender: connections_sender.clone(),
            };
            let connection_task = tokio::spawn(connection_task.run());
            connection_tasks.push(connection_task);
        }
        let acceptor = Acceptor {
            listener,
            pk,
            connection_senders,
        };
        let acceptor_task = tokio::spawn(acceptor.run());
        Ok(Self {
            acceptor_task,
            connections,
            connection_tasks,
            known_peers: peers,
        })
    }

    pub async fn shutdown(self) {
        self.connection_tasks.iter().for_each(JoinHandle::abort);
        self.acceptor_task.abort();
        let _ = join!(self.acceptor_task, join_all(self.connection_tasks));
    }

    pub fn connections(&mut self) -> &mut mpsc::Receiver<Connection> {
        &mut self.connections
    }

    pub fn known_peers(&self) -> &Vec<PeerInfo> {
        &self.known_peers
    }
}

struct PeerTask {
    peer: PeerInfo,
    active: bool,
    incoming: mpsc::Receiver<NoiseConnection>,
    pk: NoisePrivateKey,
    connections_sender: mpsc::Sender<Connection>,
}

struct ConnectionTask {
    sender: mpsc::Sender<NetworkMessage>,
    receiver: mpsc::Receiver<NetworkMessage>,
    noise_connection: NoiseConnection,
    peer: PeerInfo,
}

impl PeerTask {
    pub async fn run(mut self) {
        let mut replaced: Option<NoiseConnection> = None;
        loop {
            let source;
            let noise_connection = if let Some(replaced) = replaced.take() {
                source = "replaced";
                replaced
            } else {
                let mut establish_connection_task = tokio::spawn(Self::establish_connection(
                    self.peer.clone(),
                    self.active,
                    Duration::from_secs(1),
                    self.pk.clone(),
                ));
                let connection = select! {
                    incoming = self.incoming.recv() => {
                        source = "incoming";
                        incoming
                    },
                    outgoing = &mut establish_connection_task => {
                        source = "outgoing";
                        Some(outgoing.expect("establish_connection_task should not abort"))
                    }
                };
                establish_connection_task.abort();
                if let Some(connection) = connection {
                    connection
                } else {
                    return;
                }
            };
            let (connection, task) = self.start_connection(noise_connection);
            if self.connections_sender.send(connection).await.is_err() {
                task.abort();
                task.await.ok();
                return;
            }
            tracing::debug!(
                "Connection to {}(pub key {}) established({})",
                self.peer.index,
                self.peer.public_key,
                source
            );
            select! {
                task = task => {
                    tracing::debug!("Connection to {} dropped", self.peer.index);
                    task.ok();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                },
                incoming = self.incoming.recv() => {
                    let Some(incoming) = incoming else {
                        return;
                    };
                    tracing::warn!("Connection to {} is being replaced by concurrent incoming connection", self.peer.index);
                    replaced = Some(incoming);
                }
            }
        }
    }

    fn start_connection(&self, noise_connection: NoiseConnection) -> (Connection, JoinHandle<()>) {
        let (s1, r1) = mpsc::channel(1024);
        let (s2, r2) = mpsc::channel(1024);
        let connection = Connection {
            sender: s1,
            receiver: r2,
            peer: self.peer.clone(),
        };
        let connection_task = ConnectionTask {
            sender: s2,
            receiver: r1,
            noise_connection,
            peer: self.peer.clone(),
        };
        (connection, tokio::spawn(connection_task.run()))
    }

    async fn establish_connection(
        peer: PeerInfo,
        active: bool,
        interval: Duration,
        pk: NoisePrivateKey,
    ) -> NoiseConnection {
        let initial_delay = if active {
            Duration::ZERO
        } else {
            Duration::from_secs(3)
        };
        tokio::time::sleep(initial_delay).await;
        tracing::debug!(
            "Initiating connection to {} ({})",
            peer.index,
            if active { "immediate" } else { "delayed" }
        );
        loop {
            match Self::connect_and_handshake(&peer, &pk).await {
                Ok(connection) => return connection,
                Err(_err) => tokio::time::sleep(interval).await,
            };
        }
    }

    async fn connect_and_handshake(
        peer: &PeerInfo,
        pk: &NoisePrivateKey,
    ) -> NetworkResult<NoiseConnection> {
        let socket = TcpSocket::new_v4().expect("Failed to create socket");
        socket.set_nodelay(true).expect("Failed to set nodelay");
        let socket = socket.connect(peer.address).await?;
        let handshake = Handshake {
            pk: pk.clone(),
            socket,
        };
        let (reader, writer) = handshake.run_outgoing(&peer.public_key).await?;
        Ok(NoiseConnection {
            reader,
            writer,
            addr: peer.address,
        })
    }
}

impl ConnectionTask {
    pub async fn run(self) {
        let mut write_task = tokio::spawn(Self::write_task(
            self.receiver,
            self.noise_connection.writer,
            self.peer.clone(),
        ));
        let mut read_task = tokio::spawn(Self::read_task(
            self.sender,
            self.noise_connection.reader,
            self.peer.clone(),
        ));
        select! {
            _ = &mut write_task => {
                read_task.abort();
                read_task.await.ok();
            }
            _ = &mut read_task => {
                write_task.abort();
                write_task.await.ok();
            }
        }
    }

    async fn write_task(
        mut receiver: mpsc::Receiver<NetworkMessage>,
        mut writer: FrameWriter,
        peer: PeerInfo,
    ) {
        while let Some(message) = receiver.recv().await {
            tracing::trace!("Sending {} bytes to {}", message.data.len(), peer.index);
            if let Err(err) = writer.write_frame(&message.data).await {
                tracing::debug!("Failed to write to {}: {}", peer.public_key, err);
                return;
            }
        }
        tracing::debug!("Write task to {} complete (receiver closed)", peer.index);
    }

    async fn read_task(
        sender: mpsc::Sender<NetworkMessage>,
        mut reader: FrameReader,
        peer: PeerInfo,
    ) {
        loop {
            match reader.read_frame_encrypted().await {
                Ok(message) => {
                    tracing::trace!("Received {} bytes from {}", message.len(), peer.index);
                    let data = Bytes::copy_from_slice(message);
                    let message = NetworkMessage { data };
                    if sender.send(message).await.is_err() {
                        tracing::debug!("Read task to {} complete (sender closed)", peer.index);
                        return;
                    }
                }
                Err(err) => {
                    tracing::debug!("Failed to read from {}: {}", peer.public_key, err);
                    return;
                }
            }
        }
    }
}

impl Acceptor {
    pub async fn run(mut self) -> io::Result<()> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            let handshake = Handshake {
                pk: self.pk.clone(),
                socket,
            };
            tracing::debug!("Incoming connection from {addr}");
            // tokio::spawn(handshake.run_incoming()); // todo limit, track join handle
            match handshake.run_incoming().await {
                Ok((reader, writer, remote_key)) => {
                    let sender = self.connection_senders.get_mut(&remote_key);
                    match sender {
                        Some(sender) => {
                            tracing::debug!(
                                "Accepted incoming connection from {}, key {}",
                                addr,
                                remote_key,
                            );
                            let connection  = NoiseConnection {
                                reader,
                                writer,
                                addr
                            };
                            sender.send(connection).await.ok();
                        }
                        None => tracing::warn!("Incoming connection from {addr} with unknown static key {remote_key:?}")
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed incoming handshake: {e}");
                }
            }
        }
    }
}

impl Handshake {
    pub async fn run_incoming(self) -> NetworkResult<(FrameReader, FrameWriter, NoisePublicKey)> {
        self.socket
            .set_nodelay(true)
            .expect("Failed to set nodelay");
        let mut noise = self
            .noise_builder()
            .build_initiator()
            .expect("Failed to build initiator noise instance");
        let (reader, writer) = self.socket.into_split();
        let mut reader = FrameReader::new(reader);
        let mut writer = FrameWriter::new(writer);
        writer.write_handshake_message(&mut noise).await?;
        reader.read_handshake_message(&mut noise).await?;
        let transport_state = noise.into_transport_mode()?;
        let public_key = NoisePublicKey(
            transport_state
                .get_remote_static()
                .expect("Remote key is expected in this noise scheme")
                .into(),
        );
        let (reader, writer) = Self::finalize(reader, writer, transport_state);
        Ok((reader, writer, public_key))
    }

    pub async fn run_outgoing(
        self,
        pub_key: &NoisePublicKey,
    ) -> NetworkResult<(FrameReader, FrameWriter)> {
        let mut noise = self
            .noise_builder()
            .remote_public_key(&pub_key.0)
            .build_responder()
            .expect("Failed to build initiator noise instance");
        let (reader, writer) = self.socket.into_split();
        let mut reader = FrameReader::new(reader);
        let mut writer = FrameWriter::new(writer);
        reader.read_handshake_message(&mut noise).await?;
        writer.write_handshake_message(&mut noise).await?;
        let transport_state = noise.into_transport_mode()?;
        Ok(Self::finalize(reader, writer, transport_state))
    }

    fn noise_builder(&self) -> snow::Builder {
        snow::Builder::new(noise_params()).local_private_key(self.pk.as_ref())
    }

    fn finalize(
        mut reader: FrameReader,
        mut writer: FrameWriter,
        transport_state: TransportState,
    ) -> (FrameReader, FrameWriter) {
        let transport_state = Arc::new(Mutex::new(transport_state));
        writer.set_transport_state(transport_state.clone());
        reader.set_transport_state(transport_state.clone());
        (reader, writer)
    }
}

fn noise_params() -> NoiseParams {
    "Noise_KX_25519_ChaChaPoly_BLAKE2s".parse().unwrap()
}

impl AsRef<[u8]> for NoisePrivateKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for NoisePrivateKey {
    fn from(value: Vec<u8>) -> Self {
        Self(Arc::new(value.into_boxed_slice()))
    }
}

impl FrameReader {
    pub fn new(reader: OwnedReadHalf) -> Self {
        Self {
            reader,
            net_buffer: vec![0u8; MAX_MESSAGE].into_boxed_slice(),
            noise_buffer: vec![0u8; MAX_MESSAGE].into_boxed_slice(),
            transport_state: None,
        }
    }

    pub async fn read_handshake_message(
        &mut self,
        noise: &mut HandshakeState,
    ) -> NetworkResult<()> {
        assert!(self.transport_state.is_none());
        let handshake = Self::read_frame(&mut self.reader, &mut self.net_buffer).await?;
        let mut payload_buffer = [0u8; INIT_PAYLOAD.len()];
        let n = noise.read_message(handshake, &mut payload_buffer)?;
        let payload = &payload_buffer[..n];
        if payload != &INIT_PAYLOAD {
            return Err(NetworkError::Protocol(
                ProtocolError::InvalidHandshakePayload,
            ));
        }
        Ok(())
    }

    pub async fn read_frame<'a>(
        reader: &mut OwnedReadHalf,
        net_buffer: &'a mut [u8],
    ) -> NetworkResult<&'a [u8]> {
        let l = reader.read_u32().await? as usize;
        if l > MAX_MESSAGE {
            return Err(NetworkError::Protocol(ProtocolError::InvalidLength));
        }
        let data = &mut net_buffer[..l];
        reader.read_exact(data).await?;
        Ok(data)
    }

    pub async fn read_frame_encrypted(&mut self) -> NetworkResult<&[u8]> {
        let encrypted = Self::read_frame(&mut self.reader, &mut self.net_buffer).await?;
        Self::decrypt_frame(encrypted, &mut self.noise_buffer, &self.transport_state)
    }

    fn decrypt_frame<'a>(
        encrypted: &[u8],
        into: &'a mut [u8],
        transport_state: &Option<Arc<Mutex<TransportState>>>,
    ) -> NetworkResult<&'a [u8]> {
        let n = transport_state
            .as_ref()
            .unwrap()
            .lock()
            .read_message(encrypted, into)?;
        Ok(&into[..n])
    }

    pub fn set_transport_state(&mut self, transport_state: Arc<Mutex<TransportState>>) {
        assert!(self.transport_state.is_none());
        self.transport_state = Some(transport_state);
    }
}

impl FrameWriter {
    pub fn new(writer: OwnedWriteHalf) -> Self {
        Self {
            writer,
            buffer: vec![0u8; MAX_MESSAGE].into_boxed_slice(),
            transport_state: None,
        }
    }

    pub async fn write_handshake_message(
        &mut self,
        noise: &mut HandshakeState,
    ) -> NetworkResult<()> {
        assert!(self.transport_state.is_none());
        let n = noise.write_message(&INIT_PAYLOAD, &mut self.buffer)?;
        let buf = &self.buffer[..n];
        Self::write_frame_inner(&mut self.writer, buf).await?;
        Ok(())
    }

    pub async fn write_frame(&mut self, data: &[u8]) -> NetworkResult<()> {
        assert!(data.len() <= MAX_MESSAGE);
        let n = self
            .transport_state
            .as_ref()
            .unwrap()
            .lock()
            .write_message(&data, &mut self.buffer)?;
        // todo - encrypted size can get larger then MAX_MESSAGE
        Self::write_frame_inner(&mut self.writer, &self.buffer[..n]).await?;
        Ok(())
    }

    async fn write_frame_inner(writer: &mut OwnedWriteHalf, data: &[u8]) -> NetworkResult<()> {
        writer.write_u32(data.len() as u32).await?;
        writer.write_all(data).await?;
        Ok(())
    }

    pub fn set_transport_state(&mut self, transport_state: Arc<Mutex<TransportState>>) {
        assert!(self.transport_state.is_none());
        self.transport_state = Some(transport_state);
    }
}

type NetworkResult<T> = Result<T, NetworkError>;

pub enum NetworkError {
    Io(io::Error),
    Noise(snow::Error),
    Protocol(ProtocolError),
}

pub enum ProtocolError {
    InvalidLength,
    InvalidHandshakePayload,
}

impl From<io::Error> for NetworkError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<snow::Error> for NetworkError {
    fn from(value: snow::Error) -> Self {
        Self::Noise(value)
    }
}

impl fmt::Debug for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkError::Io(io) => write!(f, "io error: {io}"),
            NetworkError::Noise(noise) => write!(f, "noise error: {noise}"),
            NetworkError::Protocol(ProtocolError::InvalidLength) => {
                write!(f, "protocol error: invalid length")
            }
            NetworkError::Protocol(ProtocolError::InvalidHandshakePayload) => {
                write!(f, "protocol error: invalid handshake payload")
            }
        }
    }
}

impl fmt::Debug for NoisePublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl fmt::Display for NoisePublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..4]))
    }
}

impl NetworkMessage {
    pub fn new<T: Into<Bytes>>(data: T) -> Self {
        let data = data.into();
        Self { data }
    }
}

// todo - allow to pass rng
pub fn generate_network_keypair() -> (NoisePrivateKey, NoisePublicKey) {
    let builder = snow::Builder::new(noise_params());
    let keypair = builder.generate_keypair().unwrap();
    let private = NoisePrivateKey::from(keypair.private);
    let public = NoisePublicKey(keypair.public);
    (private, public)
}

#[cfg(test)]
pub struct TestConnectionPool {
    pub pools: Vec<ConnectionPool>,
    pub pub_keys: Vec<NoisePublicKey>,
    pub runtimes: Vec<tokio::runtime::Runtime>,
}

#[cfg(test)]
impl TestConnectionPool {
    pub async fn new(n: usize, base_port: usize) -> Self {
        let builder = snow::Builder::new("Noise_NN_25519_ChaChaPoly_BLAKE2s".parse().unwrap());
        let key_pairs: Vec<_> = (0..n)
            .map(|_| builder.generate_keypair().unwrap())
            .collect();
        let (pub_keys, priv_keys): (Vec<_>, Vec<_>) = key_pairs
            .into_iter()
            .map(|kp| (NoisePublicKey(kp.public), kp.private))
            .unzip();

        let peers: Vec<_> = pub_keys
            .iter()
            .enumerate()
            .map(|(i, pk)| PeerInfo {
                public_key: pk.clone(),
                address: format!("127.0.0.1:{}", base_port + i).parse().unwrap(),
                index: ValidatorIndex(i as u64),
            })
            .collect();

        let runtimes: Vec<_> = pub_keys
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let i = ValidatorIndex(i as u64);
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name(format!("[{i}]"))
                    .build()
                    .unwrap()
            })
            .collect();

        use futures::future::FutureExt;

        let pool_futures: Vec<_> = priv_keys
            .into_iter()
            .zip(peers.iter())
            .zip(runtimes.iter())
            .enumerate()
            .map(|(i, ((pk, peer), runtime))| {
                runtime.spawn(
                    ConnectionPool::start(peer.address, pk.into(), peers.clone(), i)
                        .map(|r| r.unwrap()),
                )
            })
            .collect();

        let pools = futures::future::try_join_all(pool_futures).await.unwrap();

        TestConnectionPool {
            pools,
            pub_keys,
            runtimes,
        }
    }

    pub fn into_parts<const N: usize>(
        self,
    ) -> (
        [ConnectionPool; N],
        [NoisePublicKey; N],
        Vec<tokio::runtime::Runtime>,
    ) {
        let Ok(pools) = self.pools.try_into() else {
            panic!()
        };
        let Ok(pub_keys) = self.pub_keys.try_into() else {
            panic!()
        };
        (pools, pub_keys, self.runtimes)
    }

    pub fn take_pools(&mut self) -> Vec<ConnectionPool> {
        let mut v = Vec::new();
        std::mem::swap(&mut v, &mut self.pools);
        v
    }

    pub async fn drop_wait(self) {
        Self::drop_runtimes(self.runtimes).await;
    }

    pub async fn drop_runtimes(r: Vec<tokio::runtime::Runtime>) {
        tokio::runtime::Handle::current()
            .spawn_blocking(move || drop(r))
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::enable_test_logging;
    use std::collections::HashSet;
    use tokio::time::timeout;

    #[tokio::test]
    pub async fn network_test() {
        env_logger::try_init().ok();
        let test_pool = TestConnectionPool::new(2, 8080).await;

        let ([mut pool1, mut pool2], [kpb1, kpb2], runtimes) = test_pool.into_parts();

        let c1 = timeout(Duration::from_secs(5), pool1.connections.recv())
            .await
            .unwrap()
            .unwrap();
        let mut c2 = timeout(Duration::from_secs(5), pool2.connections.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c1.peer.public_key, kpb2);
        assert_eq!(c2.peer.public_key, kpb1);

        c1.sender
            .send(NetworkMessage::new([1, 2, 3].as_ref()))
            .await
            .unwrap();
        let message = timeout(Duration::from_secs(5), c2.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.data.as_ref(), &[1, 2, 3]);
        pool1.shutdown().await;
        pool2.shutdown().await;
        TestConnectionPool::drop_runtimes(runtimes).await;
    }

    #[tokio::test]
    pub async fn network_stress_test() {
        enable_test_logging();
        let size = 20;
        let mut test_pool = TestConnectionPool::new(size, 8020).await;

        let mut tasks = vec![];
        let (s, mut r) = mpsc::channel(10);
        let mut expect_messages = HashSet::new();
        for (a, mut pool) in test_pool.take_pools().into_iter().enumerate() {
            let a = ValidatorIndex(a as u64);
            let s = s.clone();
            for b in 0..size {
                let b = ValidatorIndex(b as u64);
                if a != b {
                    expect_messages.insert((a, b));
                }
            }
            tasks.push(tokio::spawn(async move {
                while let Some(mut connection) = pool.connections.recv().await {
                    println!("{a} connected to {}", connection.peer.index);
                    let s = s.clone();
                    tokio::spawn(async move {
                        let data = vec![0, 1, 2].into();
                        connection.sender.send(NetworkMessage { data }).await.ok();
                        let d = connection.receiver.recv().await;
                        match d {
                            None => {
                                println!(
                                    "{a} did not receive anything from {}",
                                    connection.peer.index
                                );
                            }
                            Some(_d) => {
                                println!("{a} received message from {}", connection.peer.index);
                                s.send((a, connection.peer.index)).await.unwrap();
                            }
                        }
                        let () = futures::future::pending().await;
                    });
                }
            }));
        }
        println!("Expecting {} messages", expect_messages.len());
        while let Some((a, b)) = r.recv().await {
            expect_messages.remove(&(a, b));
            if expect_messages.is_empty() {
                break;
            }
            if expect_messages.len() < 10 {
                println!(
                    "Still waiting for {} messages: {expect_messages:?}",
                    expect_messages.len()
                );
            }
        }
        for task in tasks {
            task.abort();
            task.await.ok();
        }
        test_pool.drop_wait().await;
    }
}
