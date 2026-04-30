# LMDB HyperBEAM Index Explorer

Run `cargo build` and then execute `./target/debug/lmdb-explorer <lmdb_path>` to enter explorer mode.

For specific actions like block height statistics, tx information, or missing blocks, check the following commands:

## Commands
```
Interactive LMDB browser for HyperBEAM offset stores

Usage: lmdb-explorer [OPTIONS] <PATH>

Arguments:
  <PATH>  Path to the LMDB environment directory

Options:
  -l, --limit <LIMIT>    Number of entries per page (default: 20) [default: 20]
  -s, --skip <SKIP>      Skip N entries before listing (useful for pagination) [default: 0]
  -p, --prefix <PREFIX>  Filter entries by key prefix. Formats accepted: base64url  — any string of [A-Za-z0-9_-] is decoded to raw bytes (Arweave TX ID prefix) 0x<hex>    — explicit raw hex bytes otherwise  — treated as a literal UTF-8 path prefix (e.g. "data/")
      --dump             Dump all entries without interactive navigation
      --partitions       Analyze partition distribution across all keys
      --block <HEIGHT>   Show TXID count per depth for a specific block height
      --tx <TXID>        Look up a TX by base64url ID and show its parent chain and layer depth
      --missing-blocks   Find missing block ranges, scanning from highest indexed block down to 0
  -h, --help             Print help
```
