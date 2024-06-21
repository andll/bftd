use std::io::Read;
use std::net::{ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, thread};
use std::thread::JoinHandle;
use tiny_http::{Request, Response, StatusCode};
use crate::mempool::{BasicMempoolClient, MAX_TRANSACTION};

pub struct BftdServer {
    stop: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
    server: Arc<tiny_http::Server>,
}

impl BftdServer {
    pub fn start(address: impl ToSocketAddrs, mempool_client: BasicMempoolClient) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let server = Arc::new(tiny_http::Server::http(address).unwrap());
        let buffer = vec![0u8; MAX_TRANSACTION].into_boxed_slice();
        let acceptor  = BftdServerAcceptor {
            mempool_client,
            server: server.clone(),
            stop: stop.clone(),
            buffer,
        };
        let join_handle = thread::spawn(move || acceptor.run());
        Self {
            stop,
            join_handle: Some(join_handle),
            server,
        }
    }

    pub fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.server.unblock();
        self.join_handle.take().unwrap().join().unwrap();
    }
}

impl Drop for BftdServer {
    fn drop(&mut self) {
        if let Some(jh) = self.join_handle.take() {
            self.stop.store(true, Ordering::SeqCst);
            self.server.unblock();
            jh.join().unwrap();
        }
    }
}

struct BftdServerAcceptor {
    stop: Arc<AtomicBool>,
    mempool_client: BasicMempoolClient,
    server: Arc<tiny_http::Server>,
    buffer: Box<[u8]>, // todo move to worker
}

impl BftdServerAcceptor {
    pub fn run(mut self) {
        loop {
            match self.server.recv() {
                Ok(request) => {
                    match request.url() {
                        "/send" => {
                            self.handle_send(request).ok();
                        }
                        "/tail" => {

                        }
                        _ => {
                            request.respond(Response::new_empty(StatusCode(404))).ok();
                        }
                    }
                }
                Err(err) => {
                    if !self.stop.load(Ordering::SeqCst) {
                        panic!("Acceptor thread stopped with {err}")
                    }
                }
            }
        }
    }

    fn handle_send(&mut self, mut request: Request) -> io::Result<()> {
        let Some(length) = request.body_length() else {
            return request.respond(Response::new_empty(StatusCode(400)));
        };
        if length > MAX_TRANSACTION {
            return request.respond(Response::new_empty(StatusCode(400)));
        }
        let buffer = &mut self.buffer[..length];
        request.as_reader().read_exact(buffer)?;
        // todo - handle this error
        futures::executor::block_on(self.mempool_client.send_transaction(buffer.to_vec())).ok();
        Ok(())
    }
}