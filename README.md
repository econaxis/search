# Full Text Search Engine
This project is a full text search engine, based upon an on-disk, binary inverted index data structure. All queries are done on-disk, so this engine can cover many gigabytes of text with just a limited amount of memory. It can search through 10GB of Wikipedia text archives using just 50MB of physical memory.

I wrote this engine to learn how large, distributed databases can scale to many terabytes of data. Many techniques and implementation details here are inspired off Apache Lucene and Google's SSTable. I learned about ranking functions, stop word detection, inverted indices, and phrase queries from the book *Introduction to Information Retrieval*. I designed my data serialization model, like variable-length integers, delta encoding, and organization of the inverted index (into terms, positions, and frequency specific files) from studying Lucene's Java implementation. I implemented packed integer blocking/shuffling inspired by Blosc. The binary representation of my inverted index closely resembles SSTables and is inspired by LevelDB's merge-based approach (/source/compactor/Compactor.cpp). This allows parallelized index building of many gigabytes of documents while having very low memory usage.

## Features
This is a very simple, read-optimized search engine for immutable documents. It supports:
 - prefix query ("can" will return documents containing "can", "canadian", "canada", "can't", ...)
 - boolean query ("can car" will only return documents containing words with prefixes of "can" *and* "car". If no such documents exist, the engine automatically fallbacks to an *or* boolean query)
 - phrase query ("canadian car" will rank documents containing "canadian car" higher than "canadian...car." Also called positional queries)
 - sharding (the inverted index is sharded into multiple, smaller indices. This enables arbitrary scaling and multithreaded execution.)
 - multi-threaded execution (because the inverted index is horizontally-sharded across multiple SSTables, queries can execute in parallel and their results can be merged together)
 - compactation/merging (two SSTables can be merged together. Often, searching over a large SSTable is faster than searching over many smaller SSTables (in a single threaded context))
 - highlighting (since positions are stored, highlighting matches within a large document is easy)


This engine does not currently support:
 - mutability. Documents cannot be edited or removed from the index. Only additions are supported.
 - multiple fields. The whole document is indexed as one field.
 - distributed execution. Distributed execution should be easy to implement, as it'd be based off the multithreaded execution API. However, I haven't implemented it yet.
 - separation of storage and compute. The current implementation uses the filesystem as the storage layer when serving documents. However, it should be simple to enable the engine to serve arbitrary URL's rather than file paths, which decouples storage and compute. Note: the indices are still stored locally for quick access, however, this means the actual documents can be stored via S3 or another service.


