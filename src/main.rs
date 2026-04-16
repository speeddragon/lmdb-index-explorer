/// lmdb-explorer: Interactive LMDB browser for HyperBEAM offset index stores.
///
/// Keys are displayed as base64url-no-pad (Arweave ID format) when 32 bytes,
/// or as UTF-8 text for path-style keys.
///
/// Values are decoded according to the hb_store_arweave_offset encoding:
///   Byte 0:    Version (bits 7-4) | Codec (bits 3-0)
///   Bytes 1-8: StartOffset as big-endian u64
///   Bytes 9+:  Length as big-endian variable-length unsigned integer
///
/// Codec map:
///   0 → tx@1.0
///   1 → ans102@1.0
///   2 → ans104@1.0
///   3 → httpsig@1.0
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use clap::Parser;
use colored::Colorize;
use lmdb::{Cursor, Environment, EnvironmentFlags, Transaction};
use lmdb_sys as ffi;

#[derive(Parser, Debug)]
#[command(name = "lmdb-explorer", about = "Interactive LMDB browser for HyperBEAM offset stores")]
struct Args {
    /// Path to the LMDB environment directory
    #[arg(value_name = "PATH")]
    db_path: PathBuf,

    /// Number of entries per page (default: 20)
    #[arg(short, long, default_value_t = 20)]
    limit: usize,

    /// Skip N entries before listing (useful for pagination)
    #[arg(short, long, default_value_t = 0)]
    skip: usize,

    /// Filter entries by key prefix.
    /// Formats accepted:
    ///   base64url  — any string of [A-Za-z0-9_-] is decoded to raw bytes (Arweave TX ID prefix)
    ///   0x<hex>    — explicit raw hex bytes
    ///   otherwise  — treated as a literal UTF-8 path prefix (e.g. "data/")
    #[arg(short, long)]
    prefix: Option<String>,

    /// Dump all entries without interactive navigation
    #[arg(long)]
    dump: bool,

    /// Analyze partition distribution across all keys
    #[arg(long)]
    partitions: bool,
}

// ---------------------------------------------------------------------------
// Key display
// ---------------------------------------------------------------------------

/// Format a raw LMDB key for human display.
///
/// - 32-byte keys are Arweave transaction IDs → base64url-no-pad (43 chars)
/// - Valid UTF-8 keys are displayed as text
/// - Everything else falls back to lowercase hex
fn format_key(key: &[u8]) -> String {
    if key.len() == 32 {
        // Arweave native ID (raw 32 bytes) → base64url no-padding
        URL_SAFE_NO_PAD.encode(key)
    } else if let Ok(s) = std::str::from_utf8(key) {
        s.to_string()
    } else {
        hex_encode(key)
    }
}

// ---------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------

/// Codec index → human-readable name (matches hb_store_arweave_offset.erl)
fn codec_name(codec: u8) -> &'static str {
    match codec {
        0 => "tx@1.0",
        1 => "ans102@1.0",
        2 => "ans104@1.0",
        3 => "httpsig@1.0",
        _ => "unknown",
    }
}

/// Decoded representation of an LMDB value.
#[derive(Debug)]
enum DecodedValue {
    /// hb_store_arweave_offset record
    Offset {
        version: u8,
        codec: String,
        start_offset: u64,
        length: u128,
    },
    /// Special group marker used by hb_store_lmdb
    Group,
    /// Symbolic link to another path
    Link(String),
    /// Raw bytes that don't fit any known format
    Raw(Vec<u8>),
}

const PARTITION_SIZE: u64 = 3_600_000_000_000;

impl std::fmt::Display for DecodedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodedValue::Offset { version, codec, start_offset, length } => {
                let partition = start_offset / PARTITION_SIZE;
                write!(
                    f,
                    "offset  version={version}  codec={codec}  \
                     start={}  partition={}  len={length}  (end={})",
                    format!("{start_offset}").cyan(),
                    format!("{partition}").magenta(),
                    start_offset + *length as u64,
                )
            }
            DecodedValue::Group => write!(f, "<group>"),
            DecodedValue::Link(target) => write!(f, "-> {target}"),
            DecodedValue::Raw(bytes) => {
                if bytes.len() <= 64 {
                    // Show hex + ASCII side by side for short values
                    write!(f, "raw  {} | {}", hex_encode(bytes), ascii_preview(bytes))
                } else {
                    write!(
                        f,
                        "raw ({} bytes)  {}…",
                        bytes.len(),
                        hex_encode(&bytes[..32])
                    )
                }
            }
        }
    }
}

/// Try to decode an LMDB value using all known formats.
fn decode_value(val: &[u8]) -> DecodedValue {
    // --- group marker ---
    if val == b"group" {
        return DecodedValue::Group;
    }

    // --- link: prefix ---
    if let Some(rest) = val.strip_prefix(b"link:") {
        let target = String::from_utf8_lossy(rest).into_owned();
        return DecodedValue::Link(target);
    }

    // --- hb_store_arweave_offset encoding ---
    // Layout: << Format:1/binary, StartOffset:8/binary, Length/binary >>
    // Format byte: high nibble = version (should be 1), low nibble = codec (0-3)
    if val.len() >= 10 {
        let format_byte = val[0];
        let version = format_byte >> 4;
        let codec_id = format_byte & 0x0f;

        // Sanity check: version must be 1, codec 0-3
        if version == 1 && codec_id <= 3 {
            let start_offset = u64::from_be_bytes(val[1..9].try_into().unwrap());
            let length_bytes = &val[9..];

            // Decode variable-length big-endian unsigned integer
            if !length_bytes.is_empty() {
                let length = decode_unsigned(length_bytes);
                return DecodedValue::Offset {
                    version,
                    codec: codec_name(codec_id).to_string(),
                    start_offset,
                    length,
                };
            }
        }
    }

    DecodedValue::Raw(val.to_vec())
}

/// Decode a big-endian variable-length unsigned integer (all remaining bytes).
fn decode_unsigned(bytes: &[u8]) -> u128 {
    let mut result: u128 = 0;
    for &b in bytes {
        result = (result << 8) | b as u128;
    }
    result
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn ascii_preview(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' })
        .collect()
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Human-readable byte size (B / KiB / MiB / GiB / TiB).
fn format_bytes(n: u64) -> String {
    const K: u64 = 1024;
    if n < K {
        format!("{n} B")
    } else if n < K * K {
        format!("{:.2} KiB", n as f64 / K as f64)
    } else if n < K * K * K {
        format!("{:.2} MiB", n as f64 / (K * K) as f64)
    } else if n < K * K * K * K {
        format!("{:.2} GiB", n as f64 / (K * K * K) as f64)
    } else {
        format!("{:.2} TiB", n as f64 / (K * K * K * K) as f64)
    }
}

fn format_count(n: u64) -> String {
    // Insert thousands separators.
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Raw data from `mdb_env_info`.
struct EnvInfo {
    map_size: usize,
    last_pgno: usize,
    last_txnid: usize,
    max_readers: u32,
    num_readers: u32,
}

/// Call `mdb_env_info` via raw FFI (not exposed by the lmdb 0.8 safe wrapper).
fn env_info(env: &Environment) -> Option<EnvInfo> {
    let mut raw = ffi::MDB_envinfo {
        me_mapaddr: std::ptr::null_mut(),
        me_mapsize: 0,
        me_last_pgno: 0,
        me_last_txnid: 0,
        me_maxreaders: 0,
        me_numreaders: 0,
    };
    let rc = unsafe { ffi::mdb_env_info(env.env(), &mut raw) };
    if rc == 0 {
        Some(EnvInfo {
            map_size: raw.me_mapsize,
            last_pgno: raw.me_last_pgno,
            last_txnid: raw.me_last_txnid,
            max_readers: raw.me_maxreaders,
            num_readers: raw.me_numreaders,
        })
    } else {
        None
    }
}

/// Print a rich stats block for the open LMDB environment.
fn print_stats(env: &Environment, db_path: &PathBuf) {
    let stat = match env.stat() {
        Ok(s) => s,
        Err(e) => { eprintln!("  stat error: {e}"); return; }
    };
    let info = env_info(env);

    // File size from OS.
    let data_mdb = db_path.join("data.mdb");
    let file_size = std::fs::metadata(&data_mdb)
        .map(|m| m.len())
        .unwrap_or(0);

    let page_size = stat.page_size() as u64;
    let branch   = stat.branch_pages() as u64;
    let leaf     = stat.leaf_pages() as u64;
    let overflow = stat.overflow_pages() as u64;
    let pages_in_use = branch + leaf + overflow;
    let bytes_in_use = pages_in_use * page_size;
    let entries = stat.entries() as u64;

    println!("{}", "=".repeat(72));
    println!("  Storage");
    println!("{}", "-".repeat(72));
    println!("  File size:        {}  (data.mdb)", format_bytes(file_size));

    if let Some(ref i) = info {
        let map_size = i.map_size as u64;
        let pct = if map_size > 0 {
            bytes_in_use as f64 / map_size as f64 * 100.0
        } else {
            0.0
        };
        println!("  Map size:         {}  (configured limit)", format_bytes(map_size));
        println!(
            "  Map used:         {}  ({:.4}% of map)",
            format_bytes(bytes_in_use), pct
        );
    }

    println!();
    println!("  Entries");
    println!("{}", "-".repeat(72));
    println!("  Total entries:    {}", format_count(entries));
    if let Some(ref i) = info {
        println!("  Last txn ID:      {}", format_count(i.last_txnid as u64));
    }

    println!();
    println!("  B-tree");
    println!("{}", "-".repeat(72));
    println!("  Page size:        {}", format_bytes(page_size));
    println!("  Tree depth:       {}", stat.depth());
    println!("  Branch pages:     {}  ({})", format_count(branch), format_bytes(branch * page_size));
    println!("  Leaf pages:       {}  ({})", format_count(leaf), format_bytes(leaf * page_size));
    println!("  Overflow pages:   {}  ({})", format_count(overflow), format_bytes(overflow * page_size));
    println!("  Pages in use:     {}  ({})", format_count(pages_in_use), format_bytes(bytes_in_use));

    if let Some(ref i) = info {
        let total_pages = i.last_pgno as u64 + 1;
        let free_est = total_pages.saturating_sub(pages_in_use);
        println!("  Pages allocated:  {}  (last pgno + 1)", format_count(total_pages));
        println!(
            "  Free pages (est): {}  ({})",
            format_count(free_est),
            format_bytes(free_est * page_size)
        );
    }

    if let Some(ref i) = info {
        println!();
        println!("  Readers");
        println!("{}", "-".repeat(72));
        println!("  Active readers:   {} / {} slots", i.num_readers, i.max_readers);
    }

    println!("{}", "=".repeat(72));
}

// ---------------------------------------------------------------------------
// Prefix parsing
// ---------------------------------------------------------------------------

/// Return true if every character belongs to the base64url alphabet.
/// Strings containing `/`, `.`, spaces, etc. are path-style, not IDs.
fn looks_like_b64url(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Decode a (possibly partial) base64url string to its raw binary prefix.
///
/// Base64url encodes 6 bits per character. A string whose length mod 4 is:
///   0 → full groups, decodes to (n/4)*3 bytes
///   2 → 1 extra byte  (12 bits, 4 fill bits, always 0 in a valid ID)
///   3 → 2 extra bytes (18 bits, 2 fill bits)
///   1 → invalid (only 6 bits — not enough for a byte); strip one char first.
///
/// The decoded bytes are a valid binary prefix: any Arweave ID that starts
/// with the user's base64url prefix string will have those exact bytes.
fn decode_b64_prefix(s: &str) -> Option<Vec<u8>> {
    let effective_len = match s.len() % 4 {
        1 => s.len() - 1, // strip the stray char; 6 bits can't form a byte
        _ => s.len(),
    };
    if effective_len == 0 {
        return None;
    }
    URL_SAFE_NO_PAD.decode(&s[..effective_len]).ok()
}

/// Parse a user-supplied prefix string into raw LMDB key bytes.
///
/// Resolution order:
///   1. `0x<hex>`   → explicit raw bytes
///   2. base64url   → decoded binary (Arweave TX ID prefix)
///   3. otherwise   → literal UTF-8 path prefix
///
/// Also returns a human-readable description of the interpretation for display.
fn parse_prefix(input: &str) -> (Vec<u8>, String) {
    if let Some(hex) = input.strip_prefix("0x") {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .filter(|&i| i + 1 <= hex.len())
            .map(|i| u8::from_str_radix(&hex[i..i.saturating_add(2).min(hex.len())], 16).unwrap_or(0))
            .collect();
        let desc = format!("hex ({} bytes)", bytes.len());
        (bytes, desc)
    } else if looks_like_b64url(input) {
        if let Some(bytes) = decode_b64_prefix(input) {
            let desc = format!(
                "base64url → {} bytes: {}",
                bytes.len(),
                hex_encode(&bytes)
            );
            return (bytes, desc);
        }
        // Fallback: treat as UTF-8 (shouldn't normally happen for valid b64url)
        (input.as_bytes().to_vec(), format!("utf-8 ({} bytes)", input.len()))
    } else {
        (input.as_bytes().to_vec(), format!("utf-8 ({} bytes)", input.len()))
    }
}

// ---------------------------------------------------------------------------
// Entry printing
// ---------------------------------------------------------------------------

fn print_entry(index: usize, key: &[u8], val: &[u8]) {
    let key_str = format_key(key);
    let val_decoded = decode_value(val);
    println!("  [{index:>6}]  key: {}", key_str.yellow());
    println!("           val: {val_decoded}");
}

fn print_separator() {
    println!("{}", "-".repeat(80));
}

// ---------------------------------------------------------------------------
// Navigation state
// ---------------------------------------------------------------------------

struct NavState {
    page: usize,
    page_size: usize,
    prefix_filter: Option<Vec<u8>>,
}

impl NavState {
    fn offset(&self) -> usize {
        self.page * self.page_size
    }
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

fn open_env(path: &PathBuf) -> lmdb::Result<Environment> {
    Environment::new()
        .set_flags(
            // Open read-only; NO_LOCK lets us read while the HyperBEAM node
            // might have the env open for writes.
            EnvironmentFlags::READ_ONLY | EnvironmentFlags::NO_LOCK,
        )
        // 2 TiB map size matches HyperBEAM's default
        .set_map_size(2 * 1024 * 1024 * 1024 * 1024)
        .open(path)
}

/// Collect up to `limit` entries starting at `skip`, optionally filtered by
/// key prefix. Returns `(entries, has_more)`.
fn fetch_page(
    env: &Environment,
    skip: usize,
    limit: usize,
    prefix: Option<&[u8]>,
) -> lmdb::Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
    let db = env.open_db(None)?;
    let txn = env.begin_ro_txn()?;
    let mut cursor = txn.open_ro_cursor(db)?;

    let iter: Box<dyn Iterator<Item = (&[u8], &[u8])>> = match prefix {
        Some(p) => Box::new(cursor.iter_from(p)),
        None => Box::new(cursor.iter_start()),
    };

    let mut entries = Vec::new();
    let mut skipped = 0usize;
    let mut total_seen = 0usize;

    for (key, val) in iter {
        // When a prefix filter is active, stop as soon as the key no longer
        // starts with that prefix.
        if let Some(p) = prefix {
            if !key.starts_with(p) {
                break;
            }
        }

        total_seen += 1;

        if skipped < skip {
            skipped += 1;
            continue;
        }

        if entries.len() < limit {
            entries.push((key.to_vec(), val.to_vec()));
        } else {
            // Peek: at least one more entry exists
            return Ok((entries, true));
        }
    }

    let has_more = total_seen > skip + entries.len();
    Ok((entries, has_more))
}

/// Count all entries matching an optional prefix.
fn count_entries(env: &Environment, prefix: Option<&[u8]>) -> lmdb::Result<usize> {
    let db = env.open_db(None)?;
    let txn = env.begin_ro_txn()?;
    let mut cursor = txn.open_ro_cursor(db)?;

    let iter: Box<dyn Iterator<Item = (&[u8], &[u8])>> = match prefix {
        Some(p) => Box::new(cursor.iter_from(p)),
        None => Box::new(cursor.iter_start()),
    };

    let mut count = 0usize;
    for (key, _) in iter {
        if let Some(p) = prefix {
            if !key.starts_with(p) {
                break;
            }
        }
        count += 1;
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Interactive mode
// ---------------------------------------------------------------------------

fn interactive_loop(env: &Environment, args: &Args) -> lmdb::Result<()> {
    // Show stats on open.
    print_stats(env, &args.db_path);
    println!();

    let (prefix_bytes, prefix_desc): (Option<Vec<u8>>, Option<String>) =
        match args.prefix.as_ref() {
            Some(p) => {
                let (b, d) = parse_prefix(p);
                (Some(b), Some(d))
            }
            None => (None, None),
        };

    let mut nav = NavState {
        page: args.skip / args.limit,
        page_size: args.limit,
        prefix_filter: prefix_bytes.clone(),
    };

    // Use stat.entries() for total (O(1)) when no prefix filter is active;
    // fall back to iteration for filtered totals.
    let total = match nav.prefix_filter {
        None => env.stat().map(|s| s.entries()).unwrap_or(0),
        Some(_) => count_entries(env, nav.prefix_filter.as_deref())?,
    };

    print_separator();
    println!("  LMDB Explorer  —  {}", args.db_path.display());
    if let Some(p) = &args.prefix {
        let desc = prefix_desc.as_deref().unwrap_or("");
        println!("  Filter prefix:  {p}  [{desc}]");
    }
    println!("  Total entries:  {}", format_count(total as u64));
    println!("  Page size:      {}", nav.page_size);
    print_separator();

    loop {
        let skip = nav.offset();
        let (entries, has_more) =
            fetch_page(env, skip, nav.page_size, nav.prefix_filter.as_deref())?;

        let page_total = (total + nav.page_size - 1) / nav.page_size;
        println!(
            "\n  Page {} / {}  (entries {}-{})\n",
            nav.page + 1,
            page_total.max(1),
            skip + 1,
            skip + entries.len()
        );

        if entries.is_empty() {
            println!("  (no entries)");
        } else {
            for (i, (key, val)) in entries.iter().enumerate() {
                print_entry(skip + i + 1, key, val);
            }
        }

        println!();
        print_separator();
        println!(
            "  Commands:  n=next  p=prev  g <N>=goto page  \
             prefix <text>=filter  clear=clear filter  stats  q=quit"
        );
        print_separator();
        print!("  > ");
        io::stdout().flush().ok();

        let stdin = io::stdin();
        let line = {
            let mut l = String::new();
            stdin.lock().read_line(&mut l).unwrap_or(0);
            l.trim().to_string()
        };

        match line.as_str() {
            "n" | "next" => {
                if has_more {
                    nav.page += 1;
                } else {
                    println!("  (already on the last page)");
                }
            }
            "p" | "prev" => {
                if nav.page > 0 {
                    nav.page -= 1;
                } else {
                    println!("  (already on the first page)");
                }
            }
            "q" | "quit" | "exit" => break,
            "clear" => {
                nav.prefix_filter = None;
                nav.page = 0;
                println!("  Prefix filter cleared.");
            }
            "stats" => {
                println!();
                print_stats(env, &args.db_path);
                println!();
            }
            cmd if cmd.starts_with("g ") || cmd.starts_with("goto ") => {
                let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
                if let Some(n) = parts.get(1).and_then(|s| s.parse::<usize>().ok()) {
                    let page_total = (total + nav.page_size - 1) / nav.page_size;
                    if n >= 1 && n <= page_total.max(1) {
                        nav.page = n - 1;
                    } else {
                        println!("  Page {n} out of range (1-{page_total})");
                    }
                }
            }
            cmd if cmd.starts_with("prefix ") => {
                let new_prefix = cmd.strip_prefix("prefix ").unwrap_or("").trim().to_string();
                if new_prefix.is_empty() {
                    nav.prefix_filter = None;
                    println!("  Prefix filter cleared.");
                } else {
                    let (bytes, desc) = parse_prefix(&new_prefix);
                    println!("  Prefix interpreted as: {desc}");
                    nav.prefix_filter = Some(bytes);
                }
                nav.page = 0;
            }
            "" => {} // just redraw
            other => {
                println!("  Unknown command: {other:?}");
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Dump mode
// ---------------------------------------------------------------------------

fn dump_all(env: &Environment, args: &Args) -> lmdb::Result<()> {
    let prefix_bytes: Option<Vec<u8>> = args.prefix.as_ref().map(|p| {
        let (bytes, desc) = parse_prefix(p);
        println!("  Prefix: {p}  [{desc}]");
        bytes
    });

    let db = env.open_db(None)?;
    let txn = env.begin_ro_txn()?;
    let mut cursor = txn.open_ro_cursor(db)?;

    let iter: Box<dyn Iterator<Item = (&[u8], &[u8])>> =
        match prefix_bytes.as_deref() {
            Some(p) => Box::new(cursor.iter_from(p)),
            None => Box::new(cursor.iter_start()),
        };

    let mut count = 0usize;

    for (key, val) in iter {
        if let Some(p) = prefix_bytes.as_deref() {
            if !key.starts_with(p) {
                break;
            }
        }

        if count < args.skip {
            count += 1;
            continue;
        }

        print_entry(count + 1, key, val);
        count += 1;

        if count >= args.skip + args.limit {
            break;
        }
    }

    println!("\n  Total shown: {}", count.saturating_sub(args.skip));
    Ok(())
}

// ---------------------------------------------------------------------------
// Partition analysis
// ---------------------------------------------------------------------------

struct PartitionStats {
    count: u64,
    total_bytes: u128,
    min_offset: u64,
    max_offset: u64,
}

fn analyze_partitions(env: &Environment, args: &Args) -> lmdb::Result<()> {
    use std::collections::BTreeMap;

    let prefix_bytes: Option<Vec<u8>> = args.prefix.as_ref().map(|p| {
        let (bytes, desc) = parse_prefix(p);
        println!("  Prefix: {p}  [{desc}]");
        bytes
    });

    let db = env.open_db(None)?;
    let txn = env.begin_ro_txn()?;
    let mut cursor = txn.open_ro_cursor(db)?;

    let iter: Box<dyn Iterator<Item = (&[u8], &[u8])>> =
        match prefix_bytes.as_deref() {
            Some(p) => Box::new(cursor.iter_from(p)),
            None => Box::new(cursor.iter_start()),
        };

    let mut partitions: BTreeMap<u64, PartitionStats> = BTreeMap::new();
    let mut total_entries = 0u64;
    let mut offset_entries = 0u64;
    let mut other_entries = 0u64;

    for (key, val) in iter {
        if let Some(p) = prefix_bytes.as_deref() {
            if !key.starts_with(p) {
                break;
            }
        }

        total_entries += 1;

        match decode_value(val) {
            DecodedValue::Offset { start_offset, length, .. } => {
                let partition = start_offset / PARTITION_SIZE;
                let entry = partitions.entry(partition).or_insert(PartitionStats {
                    count: 0,
                    total_bytes: 0,
                    min_offset: u64::MAX,
                    max_offset: 0,
                });
                entry.count += 1;
                entry.total_bytes += length;
                if start_offset < entry.min_offset { entry.min_offset = start_offset; }
                if start_offset > entry.max_offset { entry.max_offset = start_offset; }
                offset_entries += 1;
            }
            _ => {
                other_entries += 1;
            }
        }
    }

    println!("{}", "=".repeat(88));
    println!("  Partition Distribution  —  {}", args.db_path.display());
    println!("{}", "=".repeat(88));
    println!(
        "  Total entries scanned: {}  (offset: {}  other: {})",
        format_count(total_entries),
        format_count(offset_entries),
        format_count(other_entries),
    );
    println!();

    if partitions.is_empty() {
        println!("  No offset entries found.");
    } else {
        let max_count = partitions.values().map(|s| s.count).max().unwrap_or(1);
        let bar_width = 40usize;

        println!(
            "  {:>9}  {:>11}  {:>8}  {:>20}  {:>20}  {}",
            "partition", "entries", "pct", "min offset", "max offset", "bar"
        );
        println!("{}", "-".repeat(88));

        for (&partition, stats) in &partitions {
            let pct = stats.count as f64 / offset_entries as f64 * 100.0;
            let bar_len = (stats.count as f64 / max_count as f64 * bar_width as f64).round() as usize;
            let bar = "#".repeat(bar_len);
            println!(
                "  {:>9}  {:>11}  {:>7.2}%  {:>20}  {:>20}  {}",
                format_count(partition),
                format_count(stats.count),
                pct,
                format_count(stats.min_offset),
                format_count(stats.max_offset),
                bar.cyan(),
            );
        }

        println!("{}", "-".repeat(88));

        // Summary: partition range covered
        let first_partition = *partitions.keys().next().unwrap();
        let last_partition = *partitions.keys().last().unwrap();
        let span = last_partition - first_partition + 1;
        let populated = partitions.len() as u64;
        println!(
            "  Partitions: {} populated out of {} in range [{}, {}]",
            format_count(populated),
            format_count(span),
            format_count(first_partition),
            format_count(last_partition),
        );
        println!(
            "  Partition size: {} (~{:.2} TiB)",
            format_count(PARTITION_SIZE),
            PARTITION_SIZE as f64 / (1024.0_f64.powi(4)),
        );
    }

    println!("{}", "=".repeat(88));
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let args = Args::parse();

    if !args.db_path.exists() {
        eprintln!("Error: path {:?} does not exist", args.db_path);
        std::process::exit(1);
    }

    let env = match open_env(&args.db_path) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to open LMDB environment at {:?}: {e}", args.db_path);
            std::process::exit(1);
        }
    };

    let result = if args.partitions {
        analyze_partitions(&env, &args)
    } else if args.dump {
        dump_all(&env, &args)
    } else {
        interactive_loop(&env, &args)
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
