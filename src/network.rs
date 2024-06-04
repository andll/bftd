use bytes::Bytes;
use futures::future::join_all;
use futures::join;
use parking_lot::Mutex;
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
}

#[derive(Clone)] // todo - zeroize
pub struct NoisePrivateKey(Arc<Box<[u8]>>);

#[derive(Clone)]
pub struct PeerInfo {
    pub address: SocketAddr,
    pub public_key: NoisePublicKey,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct NoisePublicKey(pub Vec<u8>);

pub struct NetworkMessage {
    pub data: Bytes,
}

pub struct Connection {
    sender: mpsc::Sender<NetworkMessage>,
    receiver: mpsc::Receiver<NetworkMessage>,
    peer: PeerInfo,
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
        })
    }

    pub async fn shutdown(self) {
        self.connection_tasks.iter().for_each(JoinHandle::abort);
        self.acceptor_task.abort();
        let _ = join!(self.acceptor_task, join_all(self.connection_tasks));
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
        loop {
            let initial_delay = if self.active {
                Duration::ZERO
            } else {
                Duration::from_secs(5)
            };
            let establish_connection_task = tokio::spawn(Self::establish_connection(
                self.peer.clone(),
                initial_delay,
                Duration::from_secs(5),
                self.pk.clone(),
            ));
            let was_incoming;
            let noise_connection = select! {
                incoming = self.incoming.recv() => {
                    was_incoming = "incoming";
                    if let Some(incoming) = incoming {
                        incoming
                    } else {
                        return;
                    }
                },
                outgoing = establish_connection_task => {
                    was_incoming = "outgoing";
                    outgoing.expect("establish_connection_task should not abort")
                }
            };
            let (connection, task) = self.start_connection(noise_connection);
            if self.connections_sender.send(connection).await.is_err() {
                task.abort();
                task.await.ok();
                return;
            }
            log::debug!(
                "Connection to {} established({})",
                self.peer.public_key,
                was_incoming
            );
            task.await.ok();
            log::debug!("Connection to {} dropped", self.peer.public_key);
            tokio::time::sleep(Duration::from_secs(1)).await;
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
        initial_delay: Duration,
        interval: Duration,
        pk: NoisePrivateKey,
    ) -> NoiseConnection {
        tokio::time::sleep(initial_delay).await;
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
            if let Err(err) = writer.write_frame(&message.data).await {
                log::debug!("Failed to write to {}: {}", peer.public_key, err);
                return;
            }
        }
    }

    async fn read_task(
        sender: mpsc::Sender<NetworkMessage>,
        mut reader: FrameReader,
        peer: PeerInfo,
    ) {
        loop {
            match reader.read_frame_encrypted().await {
                Ok(message) => {
                    let data = Bytes::copy_from_slice(message);
                    let message = NetworkMessage { data };
                    if sender.send(message).await.is_err() {
                        return;
                    }
                }
                Err(err) => {
                    log::debug!("Failed to read from {}: {}", peer.public_key, err);
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
            log::debug!("Incoming connection from {addr}");
            // tokio::spawn(handshake.run_incoming()); // todo limit, track join handle
            match handshake.run_incoming().await {
                Ok((reader, writer, remote_key)) => {
                    let sender = self.connection_senders.get_mut(&remote_key);
                    match sender {
                        Some(sender) => {
                            log::debug!(
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
                        None => log::warn!("Incoming connection from {addr} with unknown static key {remote_key:?}")
                    }
                }
                Err(e) => {
                    log::warn!("Failed incoming handshake: {e}");
                }
            }
        }
    }
}

impl Handshake {
    pub async fn run_incoming(self) -> NetworkResult<(FrameReader, FrameWriter, NoisePublicKey)> {
        self.socket.set_nodelay(true)?;
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
        snow::Builder::new("Noise_KX_25519_ChaChaPoly_BLAKE2s".parse().unwrap())
            .local_private_key(self.pk.as_ref())
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

mod test {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    pub async fn network_test() {
        env_logger::init();
        let builder = snow::Builder::new("Noise_NN_25519_ChaChaPoly_BLAKE2s".parse().unwrap());
        let kp1 = builder.generate_keypair().unwrap();
        let kp2 = builder.generate_keypair().unwrap();

        let kpb1 = NoisePublicKey(kp1.public.clone());
        let kpb2 = NoisePublicKey(kp2.public.clone());
        let peers = vec![
            PeerInfo {
                address: "127.0.0.1:8080".parse().unwrap(),
                public_key: kpb1.clone(),
            },
            PeerInfo {
                address: "127.0.0.1:8081".parse().unwrap(),
                public_key: kpb2.clone(),
            },
        ];

        let mut pool1 =
            ConnectionPool::start("127.0.0.1:8080", kp1.private.into(), peers.clone(), 0)
                .await
                .unwrap();
        let mut pool2 = ConnectionPool::start("127.0.0.1:8081", kp2.private.into(), peers, 1)
            .await
            .unwrap();

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
    }
}
