# cvdb

JSON Lines utilities for reading compact value documents.

The Rust library streams JSON values from a buffered reader. The TypeScript
module parses JSON Lines text. Both ignore blank lines and reject invalid JSON.
The Rust command reads a file or standard input and emits compact JSON Lines,
or checks the input with `--check`.

## Rust

```rust
use std::io::Cursor;

let input = Cursor::new("{\"id\":1}\n{\"id\":2}\n");
let records = cvdb::values(input).collect::<std::io::Result<Vec<_>>>()?;
assert_eq!(records.len(), 2);
# Ok::<(), std::io::Error>(())
```

## TypeScript

```typescript
import { parseJsonLines } from "@corbet-labs/cvdb";

const records = parseJsonLines('{"id":1}\n{"id":2}\n');
```

Use `jsr:@corbet-labs/cvdb` for Deno. npm ships TypeScript source and requires
a runtime or build tool with TypeScript support.

## Command

```sh
cargo install cvdb
cvdb records.jsonl
cvdb --check records.jsonl
```

With no file argument, `cvdb` reads standard input. `--check` emits the number
of valid records. Invalid JSON and input/output failures return a nonzero exit
status. Output is streamed, so an error can follow already-emitted records.
The PyPI package distributes the same native Rust command; its first wheel
supports Linux x86-64. It provides no Python import API.

## License

[Functional Source License, Version 1.1, ALv2 Future License](LICENSE.md)
(`FSL-1.1-ALv2`). Each version becomes available under Apache-2.0 two years
after it is first made available.
