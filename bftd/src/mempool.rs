use anyhow::ensure;
use bftd_core::block::{Block, MAX_BLOCK_PAYLOAD};
use bftd_core::core::ProposalMaker;
use bftd_core::syncer::BlockFilter;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;

pub struct BasicMempool {
    receiver: mpsc::Receiver<Vec<u8>>,
    last_transaction: Option<Vec<u8>>,
}
#[derive(Clone)]
pub struct BasicMempoolClient {
    sender: mpsc::Sender<Vec<u8>>,
}

pub const MAX_TRANSACTION: usize = 10 * 1024;
const TRANSACTION_SIZE_EXPECTED: usize = 256;
const TRANSACTIONS_PER_BLOCK_EXPECTED: usize = MAX_BLOCK_PAYLOAD / TRANSACTION_SIZE_EXPECTED;

impl BasicMempool {
    pub fn new() -> (BasicMempool, BasicMempoolClient) {
        let (sender, receiver) = mpsc::channel(TRANSACTIONS_PER_BLOCK_EXPECTED * 2);
        let mempool = BasicMempool {
            receiver,
            last_transaction: None,
        };
        let client = BasicMempoolClient { sender };
        (mempool, client)
    }
}

impl BasicMempoolClient {
    pub async fn send_transaction(
        &self,
        transaction: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<Vec<u8>>> {
        assert!(transaction.len() <= MAX_TRANSACTION);
        self.sender.send(transaction).await
    }
}

/// Makes sure block content can be parsed with TransactionsPayloadReader
pub struct TransactionsPayloadBlockFilter;

const PAYLOAD_HEADER_SIZE: usize = 4;
const TRANSACTION_HEADER_SIZE: usize = 4;

impl ProposalMaker for BasicMempool {
    fn make_proposal(&mut self) -> Bytes {
        let mut payload_builder = TransactionsPayloadBuilder::default();
        if let Some(transaction) = self.last_transaction.take() {
            if payload_builder.add_transaction(transaction).is_err() {
                panic!("Should be able to add at least one transaction");
            }
        }
        while let Ok(transaction) = self.receiver.try_recv() {
            if let Err(transaction) = payload_builder.add_transaction(transaction) {
                self.last_transaction = Some(transaction);
                break;
            }
        }
        payload_builder.into_payload()
    }
}

#[derive(Default)]
struct TransactionsPayloadBuilder {
    transactions: Vec<Vec<u8>>,
    size: usize,
}

impl TransactionsPayloadBuilder {
    fn add_transaction(&mut self, transaction: Vec<u8>) -> Result<(), Vec<u8>> {
        let len = transaction.len() + TRANSACTION_HEADER_SIZE;
        if self.size + len <= MAX_BLOCK_PAYLOAD - PAYLOAD_HEADER_SIZE {
            self.transactions.push(transaction);
            self.size += len;
            Ok(())
        } else {
            Err(transaction)
        }
    }

    pub fn into_payload(self) -> Bytes {
        if self.transactions.is_empty() {
            return Bytes::new();
        }
        let mut payload = BytesMut::with_capacity(self.size + PAYLOAD_HEADER_SIZE);
        payload.put_u32(self.transactions.len() as u32);
        for transaction in &self.transactions {
            payload.put_u32(transaction.len() as u32);
        }
        for transaction in self.transactions {
            payload.put_slice(&transaction);
        }
        payload.into()
    }
}

pub struct TransactionsPayloadReader {
    bytes: Bytes,
    offsets: Vec<usize>,
}

impl TransactionsPayloadReader {
    pub fn new_verify(bytes: Bytes) -> anyhow::Result<Self> {
        if bytes.len() == 0 {
            // Accept empty payload
            return Ok(Self {
                bytes,
                offsets: vec![],
            });
        }
        const MAX_LEN: usize = MAX_BLOCK_PAYLOAD / TRANSACTION_HEADER_SIZE;
        ensure!(bytes.len() >= PAYLOAD_HEADER_SIZE, "Payload too short");
        let len = u32::from_be_bytes(bytes[..PAYLOAD_HEADER_SIZE].try_into().unwrap()) as usize;
        ensure!(len <= MAX_LEN, "MAX_LEN exceeded");
        ensure!(
            bytes.len() >= PAYLOAD_HEADER_SIZE + len * TRANSACTION_HEADER_SIZE,
            "Payload does not contain all headers"
        );
        let mut offsets = Vec::with_capacity(len);
        let mut offset = len * TRANSACTION_HEADER_SIZE + PAYLOAD_HEADER_SIZE;
        for i in 0..len {
            let from = PAYLOAD_HEADER_SIZE + i * TRANSACTION_HEADER_SIZE;
            let to = PAYLOAD_HEADER_SIZE + (i + 1) * TRANSACTION_HEADER_SIZE;
            let transaction_len = u32::from_be_bytes(bytes[from..to].try_into().unwrap()) as usize;
            offsets.push(offset);
            offset += transaction_len;
        }
        ensure!(offset == bytes.len(), "Not all payload is encoded");
        Ok(Self { bytes, offsets })
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn get_bytes(&self, index: usize) -> Option<Bytes> {
        let from = *self.offsets.get(index)?;
        let to = self.index_upper_bound(index);
        Some(self.bytes.slice(from..to))
    }
    pub fn get(&self, index: usize) -> &[u8] {
        let from = *self.offsets.get(index).unwrap();
        let to = self.index_upper_bound(index);
        &self.bytes[from..to]
    }

    fn index_upper_bound(&self, index: usize) -> usize {
        if index == self.offsets.len() - 1 {
            self.bytes.len()
        } else {
            *self.offsets.get(index + 1).unwrap()
        }
    }

    pub fn iter_slices(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|i| self.get(i))
    }
}

impl BlockFilter for TransactionsPayloadBlockFilter {
    fn check_block(&self, block: &Block) -> anyhow::Result<()> {
        TransactionsPayloadReader::new_verify(block.payload_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;

    #[tokio::test]
    async fn payload_builder_test() {
        let transactions = vec![vec![1u8, 2], vec![3, 5, 6], vec![7]];
        let (mut mempool, client) = BasicMempool::new();
        let _proposal = mempool.make_proposal();
        for transaction in transactions.clone() {
            future::poll_immediate(client.send_transaction(transaction))
                .await
                .unwrap()
                .map_err(|_| ())
                .unwrap();
        }

        let proposal = mempool.make_proposal();
        let payload = TransactionsPayloadReader::new_verify(proposal).unwrap();
        assert_eq!(payload.len(), 3);
        assert_eq!(payload.get_bytes(0).unwrap().as_ref(), &[1, 2]);
        assert_eq!(payload.get_bytes(1).unwrap().as_ref(), &[3, 5, 6]);
        assert_eq!(payload.get_bytes(2).unwrap().as_ref(), &[7]);

        let proposal = mempool.make_proposal();
        let payload = TransactionsPayloadReader::new_verify(proposal).unwrap();
        assert_eq!(payload.len(), 0);

        future::poll_immediate(client.send_transaction(vec![]))
            .await
            .unwrap()
            .map_err(|_| ())
            .unwrap();
        future::poll_immediate(client.send_transaction(vec![]))
            .await
            .unwrap()
            .map_err(|_| ())
            .unwrap();
        let proposal = mempool.make_proposal();
        let payload = TransactionsPayloadReader::new_verify(proposal).unwrap();
        let empty: &[u8] = &[];
        assert_eq!(payload.len(), 2);
        assert_eq!(payload.get_bytes(0).unwrap().as_ref(), empty);
        assert_eq!(payload.get_bytes(1).unwrap().as_ref(), empty);

        let sent = MAX_BLOCK_PAYLOAD / MAX_TRANSACTION + 2;
        assert!(sent < 0xff);
        for i in 0..sent {
            let t = vec![i as u8; MAX_TRANSACTION];
            client
                .send_transaction(t.clone())
                .await
                .map_err(|_| ())
                .unwrap();
        }
        let proposal = mempool.make_proposal();
        assert!(proposal.len() <= MAX_BLOCK_PAYLOAD);
        let payload = TransactionsPayloadReader::new_verify(proposal).unwrap();
        let len1 = payload.len();
        for i in 0..len1 {
            let t = vec![i as u8; MAX_TRANSACTION];
            assert_eq!(payload.get_bytes(i).unwrap().as_ref(), &t);
        }
        let proposal = mempool.make_proposal();
        let payload = TransactionsPayloadReader::new_verify(proposal).unwrap();
        let len2 = payload.len();
        for i in 0..len2 {
            let t = vec![(i + len1) as u8; MAX_TRANSACTION];
            assert_eq!(payload.get_bytes(i).unwrap().as_ref(), &t);
        }
        assert_eq!(sent, len1 + len2);
    }
}
