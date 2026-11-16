//! The private workspace link: the access credential. Never printed, logged or put
//! in an error; stored 0600.

use crate::error::{Error, Result};
use std::fmt;
use std::io::Write;
use std::path::Path;

#[derive(Clone, PartialEq, Eq)]
pub struct Link {
    base_url: Option<String>,
    ws_id: String,
}

fn valid_id(s: &str) -> bool {
    (3..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

impl Link {
    /// Accepts a pasted link (`https://host/.../<wsId>`) or a bare workspace id. The id
    /// is the last non-empty path segment (query and fragment dropped).
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if let Some((scheme, rest)) = text.split_once("://") {
            if scheme != "https" && scheme != "http" {
                return Err(Error::BadLink);
            }
            let rest = rest.split(['?', '#']).next().unwrap_or("");
            let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
            let id = path
                .split('/')
                .rfind(|s| !s.is_empty())
                .ok_or(Error::BadLink)?;
            if host.is_empty() || !valid_id(id) {
                return Err(Error::BadLink);
            }
            Ok(Self {
                base_url: Some(format!("{scheme}://{host}")),
                ws_id: id.to_owned(),
            })
        } else if valid_id(text) {
            Ok(Self {
                base_url: None,
                ws_id: text.to_owned(),
            })
        } else {
            Err(Error::BadLink)
        }
    }

    /// The origin named by a pasted link, if any (the client uses `Config::api_base` for the API).
    #[must_use]
    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    /// The workspace id; the caller must not log it.
    #[must_use]
    pub fn ws_id(&self) -> &str {
        &self.ws_id
    }

    /// Store the link with mode 0600 (written to a sibling file and renamed).
    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        let mut f = open_private(&tmp)?;
        let text = match &self.base_url {
            Some(b) => format!("{b}/{}\n", self.ws_id),
            None => format!("{}\n", self.ws_id),
        };
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        sync_parent(path)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }
}

/// fsync the directory that holds `path`, so a rename into it survives a crash.
pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Create `dir` (and parents) with mode 0700.
pub(crate) fn create_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    Ok(())
}

pub(crate) fn open_private(path: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?)
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Link(<redacted>)")
    }
}

impl fmt::Display for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<private workspace link>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_url_and_bare_id() {
        let l = Link::parse("https://careervector.corbet.ch/abcDEF123_-xyz?x=1").unwrap();
        assert_eq!(l.ws_id(), "abcDEF123_-xyz");
        assert_eq!(l.base_url(), Some("https://careervector.corbet.ch"));
        assert_eq!(Link::parse("abc").unwrap().ws_id(), "abc");
        assert!(Link::parse("ab").is_err());
        assert!(Link::parse("ftp://h/abcdef").is_err());
        assert!(Link::parse("https://h/").is_err());
        assert!(Link::parse("bad id!").is_err());
    }

    #[test]
    fn never_printed() {
        let l = Link::parse("secretsecret123").unwrap();
        assert!(!format!("{l:?}{l}").contains("secretsecret123"));
    }

    #[test]
    fn saved_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("cvdb-link-{}", fastrand::u64(..)));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("link");
        let l = Link::parse("secretsecret123").unwrap();
        l.save(&p).unwrap();
        assert_eq!(
            std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(Link::load(&p).unwrap(), l);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
