#![forbid(unsafe_code)]

use clap::Parser;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Read and compact JSON Lines records")]
struct Args {
    /// Validate input and emit its record count instead of the records.
    #[arg(long)]
    check: bool,
    /// Input file; omit to read standard input.
    file: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let reader: Box<dyn io::BufRead> = match args.file {
        Some(file) => Box::new(BufReader::new(std::fs::File::open(file)?)),
        None => Box::new(BufReader::new(io::stdin())),
    };
    let mut output = io::BufWriter::new(io::stdout().lock());
    let mut count = 0_u64;
    for value in cvdb::values(reader) {
        let value = value?;
        if !args.check {
            serde_json::to_writer(&mut output, &value)?;
            writeln!(output)?;
        }
        count += 1;
    }
    if args.check {
        writeln!(output, "{count}")?;
    }
    output.flush()?;
    Ok(())
}
