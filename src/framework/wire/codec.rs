//! Fixed-buffer little-endian codec.
//!
//! Every payload on the wire is written through [`WireWriter`] and read
//! back through [`WireReader`]. The two are deliberately asymmetric:
//! writing targets a buffer the caller has already sized to
//! `WIRE_SIZE`, so an overflow is a contract violation and panics,
//! while reading takes whatever arrived from the network and therefore
//! bounds-checks every access.
//!
//! Byte order is little-endian throughout, and booleans are strict:
//! only `0x00` and `0x01` decode, so a corrupted flag byte fails the
//! frame instead of silently reading as `true`.

/// Why a payload could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadError {
    /// The buffer ended before the field was complete.
    TooShort,
    /// A field held a value outside its valid encoding, e.g. a boolean
    /// byte other than `0x00` or `0x01`.
    Invalid,
}

/// Fixed-buffer serialization writer. Overflow panics — callers must ensure
/// `buf.len() >= WIRE_SIZE` per trait contract.
pub struct WireWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> WireWriter<'a> {
    /// Wrap a buffer. The caller guarantees it is at least `WIRE_SIZE`
    /// bytes long for the payload being written.
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes written so far. Payload impls assert this against their
    /// declared `WIRE_SIZE`.
    pub fn written(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> &mut [u8] {
        let end = self.pos + n;
        let slice = &mut self.buf[self.pos..end];
        self.pos = end;
        slice
    }

    /// Append an unsigned byte.
    pub fn push_u8(&mut self, v: u8) {
        self.take(1)[0] = v;
    }
    /// Append a boolean as `0x00` or `0x01`.
    pub fn push_bool(&mut self, v: bool) {
        self.push_u8(v as u8);
    }
    /// Append a `u16`, little-endian.
    pub fn push_u16(&mut self, v: u16) {
        self.take(2).copy_from_slice(&v.to_le_bytes());
    }
    /// Append a `u32`, little-endian.
    pub fn push_u32(&mut self, v: u32) {
        self.take(4).copy_from_slice(&v.to_le_bytes());
    }
    /// Append a `u64`, little-endian.
    pub fn push_u64(&mut self, v: u64) {
        self.take(8).copy_from_slice(&v.to_le_bytes());
    }
    /// Append an `i16`, little-endian.
    pub fn push_i16(&mut self, v: i16) {
        self.take(2).copy_from_slice(&v.to_le_bytes());
    }
    /// Append an `i32`, little-endian.
    pub fn push_i32(&mut self, v: i32) {
        self.take(4).copy_from_slice(&v.to_le_bytes());
    }
    /// Append an `i64`, little-endian.
    pub fn push_i64(&mut self, v: i64) {
        self.take(8).copy_from_slice(&v.to_le_bytes());
    }
    /// Append an `f32` in IEEE 754 binary32, little-endian.
    pub fn push_f32(&mut self, v: f32) {
        self.take(4).copy_from_slice(&v.to_le_bytes());
    }
    /// Append an `f64` in IEEE 754 binary64, little-endian.
    pub fn push_f64(&mut self, v: f64) {
        self.take(8).copy_from_slice(&v.to_le_bytes());
    }
    /// Append a raw byte slice verbatim.
    pub fn push_bytes(&mut self, b: &[u8]) {
        self.take(b.len()).copy_from_slice(b);
    }
}

/// Fixed-buffer deserialization reader. Bounds-checked; returns
/// `PayloadError::TooShort` on underflow.
pub struct WireReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    /// Wrap a received buffer. No assumption is made about its length.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes left before the end of the buffer.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Bytes read so far since construction. Used by frame decoders to
    /// locate the CRC trailer without knowing the payload variant's size.
    pub fn consumed(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> Result<&[u8], PayloadError> {
        if self.remaining() < n {
            return Err(PayloadError::TooShort);
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// Read an unsigned byte.
    pub fn read_u8(&mut self) -> Result<u8, PayloadError> {
        Ok(self.take(1)?[0])
    }

    /// Strict boolean codec: only `0x00` and `0x01` are valid.
    pub fn read_bool(&mut self) -> Result<bool, PayloadError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PayloadError::Invalid),
        }
    }

    /// Read a `u16`, little-endian.
    pub fn read_u16(&mut self) -> Result<u16, PayloadError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    /// Read a `u32`, little-endian.
    pub fn read_u32(&mut self) -> Result<u32, PayloadError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    /// Read a `u64`, little-endian.
    pub fn read_u64(&mut self) -> Result<u64, PayloadError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// Read an `i16`, little-endian.
    pub fn read_i16(&mut self) -> Result<i16, PayloadError> {
        Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    /// Read an `i32`, little-endian.
    pub fn read_i32(&mut self) -> Result<i32, PayloadError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    /// Read an `i64`, little-endian.
    pub fn read_i64(&mut self) -> Result<i64, PayloadError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// Read an `f32`, little-endian.
    pub fn read_f32(&mut self) -> Result<f32, PayloadError> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    /// Read an `f64`, little-endian.
    pub fn read_f64(&mut self) -> Result<f64, PayloadError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// Read `n` raw bytes.
    pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], PayloadError> {
        self.take(n)
    }
}

#[cfg(test)]
mod codec_tests {
    //! The wire codec is the trust boundary against everything that
    //! arrives from the network, so the bounds checks and the strict
    //! boolean encoding are pinned here. Byte order is asserted against
    //! literal bytes rather than a round trip, because a round trip
    //! stays green even if both sides flip to big endian together.
    use super::*;

    #[test]
    fn scalars_are_written_little_endian() {
        let mut buf = [0u8; 8];
        let mut w = WireWriter::new(&mut buf);
        w.push_u16(0x1234);
        w.push_u32(0xDEAD_BEEF);
        assert_eq!(w.written(), 6);
        assert_eq!(buf[0..2], [0x34, 0x12]);
        assert_eq!(buf[2..6], [0xEF, 0xBE, 0xAD, 0xDE]);
    }

    #[test]
    fn every_scalar_type_round_trips() {
        let mut buf = [0u8; 64];
        let mut w = WireWriter::new(&mut buf);
        w.push_u8(0xAB);
        w.push_u16(0xBEEF);
        w.push_u32(0xDEAD_BEEF);
        w.push_u64(0x0123_4567_89AB_CDEF);
        w.push_i16(-2);
        w.push_i32(-3);
        w.push_i64(-4);
        w.push_f32(1.5);
        w.push_f64(-2.25);
        w.push_bool(true);
        w.push_bool(false);
        let written = w.written();

        let mut r = WireReader::new(&buf[..written]);
        assert_eq!(r.read_u8(), Ok(0xAB));
        assert_eq!(r.read_u16(), Ok(0xBEEF));
        assert_eq!(r.read_u32(), Ok(0xDEAD_BEEF));
        assert_eq!(r.read_u64(), Ok(0x0123_4567_89AB_CDEF));
        assert_eq!(r.read_i16(), Ok(-2));
        assert_eq!(r.read_i32(), Ok(-3));
        assert_eq!(r.read_i64(), Ok(-4));
        assert_eq!(r.read_f32(), Ok(1.5));
        assert_eq!(r.read_f64(), Ok(-2.25));
        assert_eq!(r.read_bool(), Ok(true));
        assert_eq!(r.read_bool(), Ok(false));
        assert_eq!(r.remaining(), 0);
        assert_eq!(r.consumed(), written);
    }

    #[test]
    fn bytes_round_trip_and_advance_the_cursor() {
        let mut buf = [0u8; 8];
        let mut w = WireWriter::new(&mut buf);
        w.push_bytes(&[1, 2, 3, 4]);
        assert_eq!(w.written(), 4);

        let mut r = WireReader::new(&buf[..4]);
        assert_eq!(r.read_bytes(4), Ok(&[1u8, 2, 3, 4][..]));
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn only_zero_and_one_decode_as_bool() {
        // A byte outside {0, 1} means the sender is not speaking our
        // protocol version, so it must be rejected instead of being
        // coerced to true.
        for v in 2u8..=255u8 {
            let buf = [v];
            let mut r = WireReader::new(&buf);
            assert_eq!(r.read_bool(), Err(PayloadError::Invalid), "byte {v:#04x}");
        }
    }

    #[test]
    fn reads_past_the_end_report_too_short() {
        let buf = [0u8; 3];
        assert_eq!(WireReader::new(&buf).read_u32(), Err(PayloadError::TooShort));
        assert_eq!(WireReader::new(&buf).read_u64(), Err(PayloadError::TooShort));
        assert_eq!(WireReader::new(&buf).read_f64(), Err(PayloadError::TooShort));
        assert_eq!(
            WireReader::new(&buf).read_bytes(4),
            Err(PayloadError::TooShort)
        );
        assert_eq!(
            WireReader::new(&[]).read_u8(),
            Err(PayloadError::TooShort)
        );
    }

    #[test]
    fn a_failed_read_does_not_move_the_cursor() {
        // Otherwise a truncated frame would leave the reader pointing
        // into the middle of the next field and a retry would silently
        // decode garbage.
        let buf = [1u8, 2, 3];
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_u32(), Err(PayloadError::TooShort));
        assert_eq!(r.consumed(), 0);
        assert_eq!(r.read_u8(), Ok(1));
    }

    #[test]
    fn exact_fit_is_accepted() {
        let buf = [0xEF, 0xBE, 0xAD, 0xDE];
        let mut r = WireReader::new(&buf);
        assert_eq!(r.read_u32(), Ok(0xDEAD_BEEF));
        assert_eq!(r.remaining(), 0);
        assert_eq!(r.read_u8(), Err(PayloadError::TooShort));
    }

    #[test]
    #[should_panic]
    fn writing_past_the_buffer_panics() {
        // Documented contract: callers guarantee `buf.len() >=
        // WIRE_SIZE`. The panic is the assertion that the contract
        // holds, so it is pinned rather than left implicit.
        let mut buf = [0u8; 2];
        let mut w = WireWriter::new(&mut buf);
        w.push_u32(1);
    }
}
