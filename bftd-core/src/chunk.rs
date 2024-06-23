use bytes::{BufMut, Bytes, BytesMut};
use std::mem;

pub struct ChunkCollector<const MAX_CHUNK: usize, const MAX_TOTAL: usize> {
    data: BytesMut,
}

impl<const MAX_CHUNK: usize, const MAX_TOTAL: usize> ChunkCollector<MAX_CHUNK, MAX_TOTAL> {
    pub fn new() -> Self {
        Self {
            data: BytesMut::with_capacity(MAX_TOTAL),
        }
    }

    pub fn try_add(&mut self, chunk: &Bytes) -> Result<(), ChunkError> {
        if self.data.len() + chunk.len() > MAX_TOTAL {
            return Err(ChunkError::MaxSizeExceeded);
        }
        self.data.put_slice(chunk);
        Ok(())
    }

    pub fn take(&mut self) -> Bytes {
        let mut data = BytesMut::with_capacity(MAX_TOTAL);
        mem::swap(&mut self.data, &mut data);
        data.into()
    }

    pub fn chunk_block(b: &Bytes) -> Vec<Bytes> {
        let chunks = (b.len() + MAX_CHUNK - 1) / MAX_CHUNK;
        let mut v = Vec::with_capacity(chunks);
        for i in 0..chunks {
            let slice = if i == chunks - 1 {
                b.slice(i * MAX_CHUNK..)
            } else {
                b.slice(i * MAX_CHUNK..(i + 1) * MAX_CHUNK)
            };
            v.push(slice);
        }
        v
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkError {
    #[error("max total size exceeded")]
    MaxSizeExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunk_test() {
        let mut c = ChunkCollector::<3, 10>::new();
        let b = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        c.try_add(&vec![1, 2, 3].into()).unwrap();
        c.try_add(&vec![4, 5, 6].into()).unwrap();
        c.try_add(&vec![7, 8, 9].into()).unwrap();
        c.try_add(&vec![10].into()).unwrap();
        assert_eq!(c.take(), b);
        assert_eq!(c.take(), Bytes::new());
        c.try_add(&vec![1, 2, 3].into()).unwrap();
        assert_eq!(c.take(), Bytes::from(vec![1, 2, 3]));

        let chunks = ChunkCollector::<3, 10>::chunk_block(&b);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks.get(0).unwrap(), &Bytes::from(vec![1, 2, 3]));
        assert_eq!(chunks.get(1).unwrap(), &Bytes::from(vec![4, 5, 6]));
        assert_eq!(chunks.get(2).unwrap(), &Bytes::from(vec![7, 8, 9]));
        assert_eq!(chunks.get(3).unwrap(), &Bytes::from(vec![10]));

        let chunks = ChunkCollector::<3, 10>::chunk_block(&b.slice(..9));
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.get(0).unwrap(), &Bytes::from(vec![1, 2, 3]));
        assert_eq!(chunks.get(1).unwrap(), &Bytes::from(vec![4, 5, 6]));
        assert_eq!(chunks.get(2).unwrap(), &Bytes::from(vec![7, 8, 9]));
    }
}
