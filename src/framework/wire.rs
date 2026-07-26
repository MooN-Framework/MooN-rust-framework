// Kleine Helfer fuer die Serialisierung von CyclePayload-Werten.
// Kapseln Bounds-Check und Offset-Tracking, damit Payload-Impls
// keine rohen Byte-Slices und `.try_into().unwrap()` brauchen.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadError {
    /// Puffer zu klein fuer die naechste Leseoperation.
    TooShort,
    /// Wert liegt ausserhalb des erlaubten Bereichs (z.B. Bool != 0/1).
    Invalid,
}

// -------------------------------------------------------------
// Writer
// -------------------------------------------------------------
//
// Panic bei Ueberlauf ist beabsichtigt: der Aufrufer garantiert per
// Trait-Vertrag `buf.len() >= WIRE_SIZE`. Ein Ueberlauf ist ein Bug in
// der Payload-Impl (WIRE_SIZE stimmt nicht mit dem to_wire-Verhalten
// ueberein), kein Laufzeitfehler.

pub struct WireWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> WireWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn written(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> &mut [u8] {
        let end = self.pos + n;
        let slice = &mut self.buf[self.pos..end];
        self.pos = end;
        slice
    }

    pub fn push_u8(&mut self, v: u8) {
        self.take(1)[0] = v;
    }
    pub fn push_bool(&mut self, v: bool) {
        self.push_u8(v as u8);
    }
    pub fn push_u16(&mut self, v: u16) {
        self.take(2).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_u32(&mut self, v: u32) {
        self.take(4).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_u64(&mut self, v: u64) {
        self.take(8).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_i16(&mut self, v: i16) {
        self.take(2).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_i32(&mut self, v: i32) {
        self.take(4).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_i64(&mut self, v: i64) {
        self.take(8).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_f32(&mut self, v: f32) {
        self.take(4).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_f64(&mut self, v: f64) {
        self.take(8).copy_from_slice(&v.to_le_bytes());
    }
    pub fn push_bytes(&mut self, b: &[u8]) {
        self.take(b.len()).copy_from_slice(b);
    }
}

// -------------------------------------------------------------
// Reader
// -------------------------------------------------------------

pub struct WireReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&[u8], PayloadError> {
        if self.remaining() < n {
            return Err(PayloadError::TooShort);
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn read_u8(&mut self) -> Result<u8, PayloadError> {
        Ok(self.take(1)?[0])
    }

    /// Strikte Bool-Codierung: nur 0x00 und 0x01 sind gueltig. Alles andere
    /// ist eine kaputte Payload — analog zum Argument bei NodeState::from_wire.
    pub fn read_bool(&mut self) -> Result<bool, PayloadError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PayloadError::Invalid),
        }
    }

    pub fn read_u16(&mut self) -> Result<u16, PayloadError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn read_u32(&mut self) -> Result<u32, PayloadError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn read_u64(&mut self) -> Result<u64, PayloadError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn read_i16(&mut self) -> Result<i16, PayloadError> {
        Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn read_i32(&mut self) -> Result<i32, PayloadError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn read_i64(&mut self) -> Result<i64, PayloadError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn read_f32(&mut self) -> Result<f32, PayloadError> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn read_f64(&mut self) -> Result<f64, PayloadError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], PayloadError> {
        self.take(n)
    }
}
