//! T1: wire fixtures produced by the `CareerVector` TS SDK (see `tests/fixtures/source.txt` for
//! the commit; refresh with `CVDB_REGEN_FIXTURES=1` in the e2e test). cvdb must decode them
//! byte for byte and the engine must read them.
use cvdb::frames::{relay_update, split_frames};
use cvws::{SubDoc, Workspace};
use serde_json::Value;
use std::path::Path;

fn fx(name: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name),
    )
    .unwrap()
}

fn text(ws: &Workspace, expected: &Value) -> String {
    ws.view(
        SubDoc::CvProfile,
        expected["branch"].as_str().unwrap(),
        expected["node"].as_str().unwrap(),
    )
    .unwrap()["data"]["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
fn framed_body_splits_into_the_sdk_updates() {
    let (u0, u1, u2) = (fx("u0.bin"), fx("u1.bin"), fx("u2.bin"));
    let body = fx("frames.bin");
    let frames = split_frames(&body, 3).unwrap();
    assert_eq!(frames, vec![u0.as_slice(), u1.as_slice(), u2.as_slice()]);
    assert!(split_frames(&body, 2).is_err());
}

#[test]
fn relay_frame_yields_the_exact_update() {
    assert_eq!(
        relay_update(&fx("relay.bin")).unwrap().unwrap(),
        fx("u2.bin")
    );
}

#[test]
fn engine_reads_the_sdk_bytes_head_then_tail() {
    let expected: Value = serde_json::from_slice(&fx("expected.json")).unwrap();
    // The framed GET body: the server's head, then the SDK's two appends.
    let body = fx("frames.bin");
    let frames = split_frames(&body, 3).unwrap();
    let mut ws = Workspace::open(&[(SubDoc::CvProfile, frames[0])]).unwrap();
    ws.receive(frames[1], Some(SubDoc::CvProfile)).unwrap();
    assert_eq!(text(&ws, &expected), expected["first"]);
    // The tail arrives as a relay frame, with no sub-document hint.
    let update = relay_update(&fx("relay.bin")).unwrap().unwrap();
    let applied = ws.receive(&update, None).unwrap();
    assert!(applied.of(SubDoc::CvProfile).is_some());
    assert_eq!(text(&ws, &expected), expected["second"]);
}
