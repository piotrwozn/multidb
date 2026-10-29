//! Nonallocating frontend grammar checks before the upstream pgwire decoder.

use std::io;

use pgwire::error::{PgWireError, PgWireResult};

fn invalid() -> PgWireError {
    io::Error::new(io::ErrorKind::InvalidData, "invalid pgwire frame body").into()
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> PgWireResult<&'a [u8]> {
        let value = self.0.get(..count).ok_or_else(invalid)?;
        self.0 = &self.0[count..];
        Ok(value)
    }

    fn string(&mut self) -> PgWireResult<&'a str> {
        let end = self
            .0
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(invalid)?;
        let value = std::str::from_utf8(self.take(end)?).map_err(|_| invalid())?;
        self.take(1)?;
        Ok(value)
    }

    fn int(&mut self) -> PgWireResult<i32> {
        let raw: [u8; 4] = self.take(4)?.try_into().map_err(|_| invalid())?;
        Ok(i32::from_be_bytes(raw))
    }

    fn count(&mut self) -> PgWireResult<usize> {
        let raw: [u8; 2] = self.take(2)?.try_into().map_err(|_| invalid())?;
        usize::try_from(i16::from_be_bytes(raw)).map_err(|_| invalid())
    }

    fn formats(&mut self) -> PgWireResult<usize> {
        let count = self.count()?;
        for _ in 0..count {
            let raw = self.take(2)?;
            if raw != [0, 0] && raw != [0, 1] {
                return Err(invalid());
            }
        }
        Ok(count)
    }

    fn end(self) -> PgWireResult<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}

pub(super) fn validate(body: &[u8], startup: bool, tag: u8) -> PgWireResult<()> {
    let mut cursor = Cursor(body);
    if startup {
        match cursor.int()? {
            80_877_102 => {
                // Cancellation accepts protocol-3.2 secret keys as well as the
                // legacy four-byte key. It never enters query dispatch.
                if !(8..=260).contains(&cursor.0.len()) {
                    return Err(invalid());
                }
                return Ok(());
            }
            196_608 | 196_610 => loop {
                if cursor.string()?.is_empty() {
                    return cursor.end();
                }
                cursor.string()?;
            },
            _ => return Err(invalid()),
        }
    }
    match tag {
        b'Q' | b'f' => {
            cursor.string()?;
        }
        b'P' => {
            cursor.string()?;
            cursor.string()?;
            let types = cursor.count()?;
            cursor.take(types.checked_mul(4).ok_or_else(invalid)?)?;
        }
        b'B' => {
            cursor.string()?;
            cursor.string()?;
            let formats = cursor.formats()?;
            let parameters = cursor.count()?;
            if formats > 1 && formats != parameters {
                return Err(invalid());
            }
            for _ in 0..parameters {
                let length = cursor.int()?;
                if length != -1 {
                    cursor.take(usize::try_from(length).map_err(|_| invalid())?)?;
                }
            }
            cursor.formats()?;
        }
        b'C' | b'D' => {
            if !matches!(cursor.take(1)?, [b'S' | b'P']) {
                return Err(invalid());
            }
            cursor.string()?;
        }
        b'E' => {
            cursor.string()?;
            if cursor.int()? < 0 {
                return Err(invalid());
            }
        }
        b'H' | b'S' | b'X' | b'c' => {}
        b'd' => return Ok(()),
        b'p' if body.len() <= 65_536 => return Ok(()),
        _ => return Err(invalid()),
    }
    cursor.end()
}

pub(super) fn validate_sasl_initial(body: &[u8]) -> PgWireResult<()> {
    let mut cursor = Cursor(body);
    cursor.string()?;
    let length = cursor.int()?;
    if length != -1 {
        cursor.take(usize::try_from(length).map_err(|_| invalid())?)?;
    }
    cursor.end()
}

#[cfg(test)]
mod tests {
    use super::{validate, validate_sasl_initial};

    #[test]
    fn malformed_lengths_counts_strings_and_trailing_bytes_fail_closed() {
        for (tag, body) in [
            (b'Q', &b"unterminated"[..]),
            (b'Q', &b"select 1\0junk"[..]),
            (b'P', &b"\0\0"[..]),
            (b'P', &b"\0\0\x7f\xff"[..]),
            (b'B', &b"\0\0\0\0\0\0\xff\xff"[..]),
            (b'B', &b"\0\0\0\0\0\x01\xff\xff\xff\xfe\0\0"[..]),
            (b'B', &b"\0\0\0\x01\0\x02\0\0\0\0"[..]),
            (b'E', &b"\0\xff\xff\xff\xff"[..]),
            (b'C', &b"X\0"[..]),
            (b'S', &b"junk"[..]),
        ] {
            assert!(validate(body, false, tag).is_err(), "{tag}: {body:?}");
        }
        assert!(validate_sasl_initial(b"SCRAM-SHA-256\0\xff\xff\xff\xfe").is_err());
        assert!(validate_sasl_initial(b"SCRAM-SHA-256\0\0\0\0\x10abc").is_err());
        assert!(validate_sasl_initial(b"SCRAM-SHA-256").is_err());
    }

    #[test]
    fn valid_frontend_frames_and_sasl_are_accepted() {
        for (tag, body) in [
            (b'Q', &b"select 1\0"[..]),
            (b'P', &b"\0select 1\0\0\0"[..]),
            (b'B', &b"\0\0\0\0\0\x01\xff\xff\xff\xff\0\0"[..]),
            (b'E', &b"\0\0\0\0\0"[..]),
            (b'C', &b"S\0"[..]),
            (b'S', &b""[..]),
        ] {
            assert!(validate(body, false, tag).is_ok(), "{tag}: {body:?}");
        }
        assert!(validate_sasl_initial(b"SCRAM-SHA-256\0\0\0\0\x03abc").is_ok());
        assert!(
            validate(
                &[0, 3, 0, 0, b'u', b's', b'e', b'r', 0, b'a', 0, 0],
                true,
                0
            )
            .is_ok()
        );
    }
}
