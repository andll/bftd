use crate::channel_store::ChannelId;
use anyhow::bail;
use bytes::{BufMut, Bytes, BytesMut};

pub struct ChannelPayload {
    channels: Vec<ChannelId>,
    payload: Bytes,
}

impl ChannelPayload {
    pub const MAX_CHANNELS_PER_PAYLOAD: usize = 127;

    pub fn new(channels: Vec<ChannelId>, payload: Bytes) -> Self {
        Self { channels, payload }
    }

    pub fn from_bytes(bytes: &Bytes) -> anyhow::Result<Self> {
        if bytes.len() < 1 {
            bail!("Payload too small");
        }
        let count = bytes[0] as usize;
        if count > Self::MAX_CHANNELS_PER_PAYLOAD {
            bail!("Too many channels in payload");
        }
        if bytes.len() < ChannelId::LENGTH * count + 1 {
            bail!("Payload too small");
        }
        let mut channels = Vec::with_capacity(count);
        for i in 0..count {
            let from = 1 + i * ChannelId::LENGTH;
            let to = 1 + (i + 1) * ChannelId::LENGTH;
            channels.push(ChannelId::from_slice(&bytes[from..to]));
        }
        let payload = bytes.slice(1 + ChannelId::LENGTH * count..);
        Ok(Self { channels, payload })
    }

    pub fn into_byte_vec(self) -> Vec<u8> {
        // todo redundant memory copy
        let mut data = BytesMut::with_capacity(
            self.payload.len() + 1 + self.channels.len() * ChannelId::LENGTH,
        );
        assert!(self.channels.len() <= Self::MAX_CHANNELS_PER_PAYLOAD);
        data.put_u8(self.channels.len() as u8);
        for channel in self.channels {
            data.put_u32(channel.as_u32());
        }
        data.put_slice(&self.payload);
        data.into()
    }

    pub fn channels(&self) -> &[ChannelId] {
        &self.channels
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}
