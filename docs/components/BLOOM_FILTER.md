# Bloom Filter

A Bloom filter is a space-efficient probabilistic data structure used to test whether an element is a member of a set.

## Usage in FlashDB
FlashDB attaches a Bloom filter to every SSTable file.
- **Purpose**: To avoid unnecessary disk I/O by checking the filter before reading data blocks from the SSTable.
- **Properties**: 
  - **No false negatives**: If the filter says a key is absent, it is definitely absent.
  - **Possible false positives**: If the filter says a key is present, it *might* be present.

## Configuration
- **False Positive Rate (FPR)**: Configurable per SSTable (default 1%).
- **Adaptive FPR**: FlashDB v4 monitors observed false positive rates and automatically adjusts the filter size for new SSTables to optimize the memory-vs-performance trade-off.

## Implementation
- Uses the **Kirsch-Mitzenmacher** technique to simulate multiple hash functions using only two base hashes.
- Sized optimally based on the number of entries in the MemTable at flush time.
