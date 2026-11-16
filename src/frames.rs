//! Wire codecs: var-uint8-array frames (the framed `GET /sub` body) and relay sync frames.

use crate::error::{Error, Result};

/// Frames over this size are refused without being decoded (the `cvws` cap).
pub const MAX_FRAME: usize = 1 << 20;

/// The first frame of a framed body is the stored head, a whole state: `cvws` puts no cap on
/// whole states, so it gets a larger one (heads are about 20 KB today). Merged tails and relay
/// frames stay under [`MAX_FRAME`].
pub const MAX_HEAD: usize = 32 << 20;

/// Relay tag: Yjs sync.
pub const MSG_SYNC: u64 = 0;
const SYNC_STEP2: u64 = 1;
const SYNC_UPDATE: u64 = 2;

pub fn write_var_uint(out: &mut Vec<u8>, mut n: u64) {
    while n > 0x7f {
        out.push(0x80 | (n & 0x7f) as u8);
        n >>= 7;
    }
    out.push(n as u8);
}

pub fn read_var_uint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut n: u64 = 0;
    let mut shift = 0;
    loop {
        let b = *buf
            .get(*pos)
            .ok_or_else(|| Error::Protocol("truncated varint".into()))?;
        *pos += 1;
        n |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(n);
        }
        shift += 7;
        if shift > 49 {
            return Err(Error::Protocol("varint too long".into()));
        }
    }
}

pub fn write_var_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_var_uint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn read_var_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    read_var_bytes_capped(buf, pos, MAX_FRAME)
}

fn read_var_bytes_capped<'a>(buf: &'a [u8], pos: &mut usize, cap: usize) -> Result<&'a [u8]> {
    let len =
        usize::try_from(read_var_uint(buf, pos)?).map_err(|_| Error::Protocol("length".into()))?;
    if len > cap {
        return Err(Error::Protocol("frame over the cap".into()));
    }
    let end = pos
        .checked_add(len)
        .filter(|e| *e <= buf.len())
        .ok_or_else(|| Error::Protocol("truncated frame".into()))?;
    let slice = &buf[*pos..end];
    *pos = end;
    Ok(slice)
}

/// Split a framed `GET /sub` body into its frames (head first). `expected` is the
/// `x-cv-update-frames` header; a body that does not hold exactly that many frames is a
/// protocol error. A frame over the cap is refused here, before anything decodes it.
pub fn split_frames(body: &[u8], expected: usize) -> Result<Vec<&[u8]>> {
    let mut pos = 0;
    let mut out = Vec::new();
    while pos < body.len() {
        let cap = if out.is_empty() { MAX_HEAD } else { MAX_FRAME };
        out.push(read_var_bytes_capped(body, &mut pos, cap)?);
        if out.len() > expected {
            return Err(Error::Protocol("more frames than announced".into()));
        }
    }
    if out.len() != expected {
        return Err(Error::Protocol("fewer frames than announced".into()));
    }
    Ok(out)
}

/// The inverse of [`split_frames`] (fixtures and tests).
#[must_use]
pub fn encode_frames(frames: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frames {
        write_var_bytes(&mut out, f);
    }
    out
}

/// A relay frame as the Durable Object fans it out: tag 0, `messageYjsUpdate` (2), var-uint8-array.
#[must_use]
pub fn encode_relay_update(update: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_var_uint(&mut out, MSG_SYNC);
    write_var_uint(&mut out, SYNC_UPDATE);
    write_var_bytes(&mut out, update);
    out
}

/// The update bytes of a relay frame, or `None` for frames we ignore (awareness, p2p
/// signals, sync step 1). Malformed or oversize frames are `Err`, never decoded.
pub fn relay_update(frame: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut pos = 0;
    if read_var_uint(frame, &mut pos)? != MSG_SYNC {
        return Ok(None);
    }
    let kind = read_var_uint(frame, &mut pos)?;
    if kind != SYNC_UPDATE && kind != SYNC_STEP2 {
        return Ok(None);
    }
    Ok(Some(read_var_bytes(frame, &mut pos)?.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for n in [0u64, 1, 127, 128, 300, 1 << 20, 1 << 40] {
            let mut b = Vec::new();
            write_var_uint(&mut b, n);
            assert_eq!(read_var_uint(&b, &mut 0).unwrap(), n);
        }
    }

    #[test]
    fn frames_roundtrip_and_count() {
        let body = encode_frames(&[b"head", b"", b"tail"]);
        let f = split_frames(&body, 3).unwrap();
        assert_eq!(f, vec![&b"head"[..], b"", b"tail"]);
        assert!(split_frames(&body, 2).is_err());
        assert!(split_frames(&body, 4).is_err());
        assert!(split_frames(&body[..body.len() - 1], 3).is_err());
    }

    #[test]
    fn oversize_frame_refused_before_decoding() {
        // A tail frame over the cap is refused; the head (a whole state) has the larger cap.
        let mut body = encode_frames(&[b"head"]);
        write_var_uint(&mut body, (MAX_FRAME + 1) as u64);
        body.extend(std::iter::repeat_n(0u8, 16));
        assert!(split_frames(&body, 2).is_err());
        let mut head = Vec::new();
        write_var_uint(&mut head, (MAX_HEAD + 1) as u64);
        head.extend(std::iter::repeat_n(0u8, 16));
        assert!(split_frames(&head, 1).is_err());
    }

    #[test]
    fn relay_frames() {
        let f = encode_relay_update(b"abc");
        assert_eq!(f, [0, 2, 3, b'a', b'b', b'c']);
        assert_eq!(relay_update(&f).unwrap().unwrap(), b"abc");
        assert_eq!(relay_update(&[1, 9, 9]).unwrap(), None);
        assert_eq!(relay_update(&[0, 0, 1, 9]).unwrap(), None);
        assert!(relay_update(&[0, 2, 9, 1]).is_err());
        assert!(relay_update(&[]).is_err());
    }
}
