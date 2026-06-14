//! Mirror of `src/storage/wal.rs` unit tests, expressed through the public API.

use anyhow::Result;
use std::fs::File;
use tempfile::TempDir;

use strata_query::storage::wal::{replay_wal, WalWriter};
use strata_query::Row;

#[test]
fn test_wal_write_and_replay() -> Result<()> {
    let dir = TempDir::new()?;
    let wal_path = dir.path().join("test.wal");

    // Write rows
    {
        let mut wal = WalWriter::create(&wal_path)?;
        let mut row1 = Row::new();
        row1.insert("name".into(), "alice".into());
        row1.insert("age".into(), "30".into());
        wal.append(&row1)?;

        let mut row2 = Row::new();
        row2.insert("name".into(), "bob".into());
        row2.insert("age".into(), "25".into());
        wal.append(&row2)?;
    }

    // Replay
    let rows = replay_wal(&wal_path)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("name").unwrap(), "alice");
    assert_eq!(rows[1].get("age").unwrap(), "25");

    // Truncate
    let wal = WalWriter::create(&wal_path)?;
    wal.truncate()?;

    // WAL should be gone
    assert!(!WalWriter::exists_and_nonempty(&wal_path));

    Ok(())
}

#[test]
fn test_wal_empty() -> Result<()> {
    let dir = TempDir::new()?;
    let wal_path = dir.path().join("empty.wal");

    assert!(!WalWriter::exists_and_nonempty(&wal_path));

    // Create empty WAL
    File::create(&wal_path)?;
    assert!(!WalWriter::exists_and_nonempty(&wal_path));

    Ok(())
}

#[test]
fn replay_missing_wal_is_an_error() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("nope.wal");
    // No file exists; callers gate on exists_and_nonempty first, but the
    // raw replay surfaces the open failure rather than silently succeeding.
    assert!(replay_wal(&wal_path).is_err());
    assert!(!WalWriter::exists_and_nonempty(&wal_path));
}

#[test]
fn crash_without_truncate_leaves_wal_for_replay() -> Result<()> {
    let dir = TempDir::new()?;
    let wal_path = dir.path().join("crash.wal");

    // Simulate a crash: append rows, then drop the writer WITHOUT calling
    // truncate(). The WAL file must persist with all appended rows.
    {
        let mut wal = WalWriter::create(&wal_path)?;
        for i in 0..5 {
            let mut row = Row::new();
            row.insert("id".into(), i.to_string());
            wal.append(&row)?;
        }
        wal.sync()?;
        // wal dropped here, no truncate
    }

    assert!(WalWriter::exists_and_nonempty(&wal_path));

    // Recovery replays every row, in append order.
    let rows = replay_wal(&wal_path)?;
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get("id").unwrap(), &i.to_string());
    }

    Ok(())
}

#[test]
fn replay_skips_blank_lines() -> Result<()> {
    let dir = TempDir::new()?;
    let wal_path = dir.path().join("blanks.wal");

    // Hand-write a WAL with interleaved blank lines (e.g. a partial flush).
    {
        use std::io::Write;
        let mut f = File::create(&wal_path)?;
        writeln!(f, "{{\"a\":\"1\"}}")?;
        writeln!(f)?; // blank
        writeln!(f, "{{\"a\":\"2\"}}")?;
        writeln!(f, "   ")?; // whitespace-only
    }

    let rows = replay_wal(&wal_path)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("a").unwrap(), "1");
    assert_eq!(rows[1].get("a").unwrap(), "2");

    Ok(())
}

#[test]
fn append_reopens_and_accumulates() -> Result<()> {
    // create() opens in append mode, so a second writer over the same path
    // adds to, rather than truncates, the existing log.
    let dir = TempDir::new()?;
    let wal_path = dir.path().join("accumulate.wal");

    {
        let mut wal = WalWriter::create(&wal_path)?;
        let mut row = Row::new();
        row.insert("k".into(), "first".into());
        wal.append(&row)?;
    }
    {
        let mut wal = WalWriter::create(&wal_path)?;
        let mut row = Row::new();
        row.insert("k".into(), "second".into());
        wal.append(&row)?;
    }

    let rows = replay_wal(&wal_path)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("k").unwrap(), "first");
    assert_eq!(rows[1].get("k").unwrap(), "second");

    Ok(())
}
