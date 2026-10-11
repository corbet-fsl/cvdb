use std::io::{self, Cursor, Read};

#[test]
fn blank_lines_crlf_and_unicode_preserve_the_records() {
    let input = Cursor::new("\n{\"name\":\"雪\"}\r\n \r\nnull\n[1,true]\n");
    let values = cvdb::values(input).collect::<io::Result<Vec<_>>>().unwrap();
    assert_eq!(
        values,
        vec![
            serde_json::json!({"name": "雪"}),
            serde_json::Value::Null,
            serde_json::json!([1, true])
        ]
    );
}

#[test]
fn malformed_records_are_reported_without_hiding_later_records() {
    let mut records = cvdb::values(Cursor::new("1\n{invalid}\n2\n"));
    assert_eq!(records.next().unwrap().unwrap(), 1);
    assert!(records.next().unwrap().is_err());
    assert_eq!(records.next().unwrap().unwrap(), 2);
}

#[test]
fn reader_errors_are_not_treated_as_end_of_input() {
    assert!(cvdb::values(FailedRead).next().unwrap().is_err());
}

struct FailedRead;

impl Read for FailedRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("input unavailable"))
    }
}

impl io::BufRead for FailedRead {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        Err(io::Error::other("input unavailable"))
    }
    fn consume(&mut self, _: usize) {}
}
