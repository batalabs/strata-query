//! Write-Ahead Log (WAL) for crash recovery.
//!
//! Before rows are written to segments, they are appended to a WAL file.
//! If the process crashes before `flush_all()`, the WAL is replayed on
//! next startup to recover in-flight rows.
//!
//! # Format
//!
//! One JSON line per row: `{"fields":{"col1":"val1","col2":"val2"}}`
//!
//! # Lifecycle
//!
//! 1. `WalWriter::create(path)`: creates or opens the WAL file
//! 2. `wal.append(&row)`: writes a row to the WAL (fsync optional)
//! 3. `wal.truncate()`: called after a successful `flush_all()`
//! 4. `WalReader::replay(path)`: reads all rows from the WAL on startup

use crate::types::Row;
use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// Write-ahead log writer. Appends rows to a log file.
pub struct WalWriter {
    file: File,
    path: std::path::PathBuf,
}

impl WalWriter {
    /// Create or open a WAL file for appending.
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("Failed to open WAL at {:?}", path))?;
        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Append a row to the WAL. Each row is a single JSON line.
    pub fn append(&mut self, row: &Row) -> Result<()> {
        // Serialize row fields as JSON
        let json = serde_json::to_string(&row.fields).context("Failed to serialize row for WAL")?;
        writeln!(self.file, "{}", json).context("Failed to write to WAL")?;
        Ok(())
    }

    /// Sync the WAL to disk for durability.
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all().context("Failed to fsync WAL")
    }

    /// Truncate the WAL after a successful flush.
    /// This marks all rows as safely persisted in segment files.
    pub fn truncate(self) -> Result<()> {
        // Close the file and delete the WAL
        drop(self.file);
        std::fs::remove_file(&self.path)
            .or_else(|e| {
                // Ignore "file not found" (already truncated)
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .with_context(|| format!("Failed to truncate WAL at {:?}", self.path))?;
        Ok(())
    }

    /// Check if a WAL file exists and has content.
    pub fn exists_and_nonempty(path: &Path) -> bool {
        if !path.exists() {
            return false;
        }
        match std::fs::metadata(path) {
            Ok(meta) => meta.len() > 0,
            Err(_) => false,
        }
    }
}

/// Replay rows from a WAL file for crash recovery.
pub fn replay_wal(path: &Path) -> Result<Vec<Row>> {
    let file =
        File::open(path).with_context(|| format!("Failed to open WAL for replay at {:?}", path))?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result.with_context(|| format!("Failed to read WAL line {}", line_num))?;

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields: std::collections::HashMap<String, String> = serde_json::from_str(line)
            .with_context(|| format!("Failed to parse WAL line {}: {}", line_num, line))?;

        let mut row = Row::new();
        for (k, v) in fields {
            row.insert(k, v);
        }
        rows.push(row);
    }

    Ok(rows)
}
