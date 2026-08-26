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

        // Skip zero-length frames (compat with keepalive packets)
        if len == 0 {
            src.advance(4);
            return Ok(Some(Vec::new()));
        }

        const MAX_PACKET_SIZE: usize = 8 * 1024 * 1024;
        // Safety limit: max 8MB per packet
        if len > MAX_PACKET_SIZE {
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

    #[test]
    fn test_decode_zero_length_frame() {
        // keepalive packets: length 0 must decode to an empty frame
        let mut codec = CanalCodec;
        let mut buf = BytesMut::from(&[0, 0, 0, 0][..]);
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(Vec::new()));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_decode_rejects_oversize_packet() {
        let mut codec = CanalCodec;
        // header claims 0x00800001 = 8MB + 1, above the 8MB safety limit
        let mut buf = BytesMut::from(&[0x00, 0x80, 0x00, 0x01][..]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, CanalError::Protocol(_)));
        assert_eq!(buf.len(), 4); // header preserved on error
    }

    #[test]
    fn test_decode_max_size_header_waits_for_payload() {
        let mut codec = CanalCodec;
        // exactly 8MB is allowed: must wait for payload, not error
        let mut buf = BytesMut::new();
        buf.put_u32(8 * 1024 * 1024);
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_decode_exact_boundary_payload() {
        let mut codec = CanalCodec;
        // exactly 4 + len bytes available: decodes in a single call
        let mut buf = BytesMut::from(&[0, 0, 0, 2, 1, 2][..]);
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(vec![1, 2]));
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_encode_empty_payload_roundtrip() {
        let mut codec = CanalCodec;
        let mut buf = BytesMut::new();
        codec.encode(Vec::new(), &mut buf).unwrap();
        assert_eq!(buf.len(), 4);
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(Vec::new()));
    }

    #[test]
    fn test_encode_large_payload_roundtrip() {
        let mut codec = CanalCodec;
        let mut buf = BytesMut::new();
        let payload = vec![7u8; 64 * 1024]; // 64KB payload
        codec.encode(payload.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap();
        assert_eq!(decoded, Some(payload));
    }

    #[test]
    fn test_decode_multiple_packets_with_zero_length_frame() {
        let mut codec = CanalCodec;
        // [len=1, 42] [len=0 keepalive] [len=1, 99]
        let mut buf = BytesMut::from(&[0, 0, 0, 1, 42, 0, 0, 0, 0, 0, 0, 0, 1, 99][..]);

        assert_eq!(codec.decode(&mut buf).unwrap(), Some(vec![42]));
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Vec::new()));
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(vec![99]));
        assert_eq!(buf.len(), 0);
    }
}
