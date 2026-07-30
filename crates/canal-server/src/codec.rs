use bytes::{Buf, BufMut, BytesMut};
use canal_common::CanalError;
use tokio_util::codec::{Decoder, Encoder};

/// Raw packet bytes (protobuf-encoded Packet, to be decoded by caller)
pub type PacketBytes = Vec<u8>;

/// Canal TCP wire protocol codec.
///
/// Format: `[4 bytes BE length][protobuf Packet payload]`
/// Corresponds to Java Netty `LengthFieldBasedFrameDecoder` + `ProtobufDecoder`
#[derive(Default)]
pub struct CanalCodec;

impl CanalCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Decoder for CanalCodec {
    type Item = PacketBytes;
    type Error = CanalError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for the length header
        if src.len() < 4 {
            return Ok(None);
        }

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Safety limit: max 64MB per packet
        if len > 64 * 1024 * 1024 {
            return Err(CanalError::Protocol(format!(
                "packet too large: {} bytes",
                len
            )));
        }

        // Wait for full payload
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        // Extract: skip 4-byte header, take len bytes of payload
        src.advance(4);
        let payload = src[..len].to_vec();
        src.advance(len);
        Ok(Some(payload))
    }
}

impl Encoder<PacketBytes> for CanalCodec {
    type Error = CanalError;

    fn encode(&mut self, item: PacketBytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.len() as u32;
        dst.reserve(4 + item.len());
        dst.put_u32(len);
        dst.put_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_complete_packet() {
        let mut codec = CanalCodec;
        // [0,0,0,5] = length 5, then "hello"
        let mut buf = BytesMut::from(&[0, 0, 0, 5, 104, 101, 108, 108, 111][..]);
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));
        assert_eq!(buf.len(), 0); // all consumed
    }

    #[test]
    fn test_decode_incomplete_header() {
        let mut codec = CanalCodec;
        let mut buf = BytesMut::from(&[0, 0][..]);
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, None);
        assert_eq!(buf.len(), 2); // preserved
    }

    #[test]
    fn test_decode_incomplete_payload() {
        let mut codec = CanalCodec;
        // Length says 10 bytes but only 3 bytes of data
        let mut buf = BytesMut::from(&[0, 0, 0, 10, 1, 2, 3][..]);
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, None);
        assert_eq!(buf.len(), 7); // all 7 bytes preserved for next read
    }

    #[test]
    fn test_encode_roundtrip() {
        let mut codec = CanalCodec;
        let mut buf = BytesMut::new();
        let payload = b"test-payload".to_vec();

        codec.encode(payload.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap();

        assert_eq!(decoded, Some(payload));
    }

    #[test]
    fn test_decode_multiple_packets() {
        let mut codec = CanalCodec;
        // Two packets: [0,0,0,1,42] and [0,0,0,1,99]
        let mut buf = BytesMut::from(&[0, 0, 0, 1, 42, 0, 0, 0, 1, 99][..]);

        let pkt1 = codec.decode(&mut buf).unwrap();
        assert_eq!(pkt1, Some(vec![42]));

        let pkt2 = codec.decode(&mut buf).unwrap();
        assert_eq!(pkt2, Some(vec![99]));

        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_header_is_big_endian() {
        let mut codec = CanalCodec;
        let mut buf = BytesMut::new();

        // 300 bytes = 0x0000012C in big-endian
        let payload = vec![0u8; 300];
        codec.encode(payload, &mut buf).unwrap();

        assert_eq!(buf[0], 0);
        assert_eq!(buf[1], 0);
        assert_eq!(buf[2], 1);
        assert_eq!(buf[3], 0x2C);
        assert_eq!(buf.len(), 304);
    }
}
