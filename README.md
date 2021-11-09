# Full Text Search Engine
This project is a full text search engine, based upon an on-disk, binary inverted index data structure. All queries are done on-disk, so we can search through many gigabytes of text with constant memory usage (~50MB).

## Demo
The entire English wikibooks dataset (around 800 MB of pure text) is searched.

https://user-images.githubusercontent.com/25783926/140873586-c9c587d3-8138-4c32-a1fa-7fcf1289238e.mp4

As of November 2021, I have completely revamed the ranking metrics, indexing method, storage engine, web server, and website UI. These improvements enabled me to show "previews" and highlight relevant parts. More information on the storage engine is further down.



### Old Demo (before November 2021)
Around 50,000 Instructables documents are searched near-instantly as the user types. 
![image](/demo.gif)


These queries are run in parallel across independent, horizontal-sharded indices. 

## Inspirations
I wrote this engine to learn how large, distributed databases can scale to many terabytes of data. Many techniques and implementation details here are inspired off Apache Lucene and Google's SSTable. I learned about ranking functions, stop word detection, inverted indices, and phrase queries from the book *Introduction to Information Retrieval*. I designed my data serialization model, like variable-length integers, delta encoding, and organization of the inverted index (into terms, positions, and frequency specific files) from studying Lucene's Java implementation. I implemented packed integer blocking/shuffling inspired by Blosc. The binary representation of my inverted index closely resembles SSTables and is inspired by LevelDB's merge-based approach (/source/compactor/Compactor.cpp). This allows parallelized index building of many gigabytes of documents while having very low memory usage.

# Features
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
 - (note: as of Nov 2021, this has been implemented) separation of storage and compute. The current implementation uses the filesystem as the storage layer when serving documents. However, it should be simple to enable the engine to serve arbitrary URL's rather than file paths, which decouples storage and compute. Note: the indices are still stored locally for quick access, however, this means the actual documents can be stored via S3 or another service.


# Cool Implementation Tidbits
## *New!* Improved storage engine

The search engine only tracks 32 bit document ID's. This allows quicker searching and reduced memory pressure. However, we need some way to translate those ID's into actual user data, like URL's, names, or documents. Before, I coded a simple but inefficient storage engine that stored ID's and their corresponding URL's consecutively. Searching would be O(n). 

This quick design was the performance bottleneck and lacked extensibility. What if I wanted to store more than URL's, like actual, large documents? I experimented briefly with SQLite, which worked more than enough for my needs. However, after making this whole search engine myself, I had an itch to replace SQLite with my own database.

Therefore, I created my own database with lists of homogenous, fixed-size structs as the fundamental data type. This is similar to a table in SQLite. Since all rows are fixed size, rows requiring variable length data, like textual documents or URL's, place their data on an on-disk heap, and store 64 bit offsets (like pointers) into the heap. 

I knew my access patterns well. Documents will be inserted in monotonically increasing order of ID. Documents will always be queried by ID. There will be no mutations. Therefore, I coded these access patterns straight into the database itself. In addition, I already knew that I'd need to store ID's, and two variable length strings: the URL and the actual content. Therefore, I compiled that struct definition with the database. There are only two functions: `read(id) -> string` and `put(id, url, contents`. These decisions saved me from writing query parsers, runtime struct offset calculations, or runtime type checks.

The database is at `/rust/db1/` in this repo, and is written in Rust. The design is a much simpler version of a log-structured merge tree, because my usage doesn't need the more advanced techniques of an LSM. I learned many things by writing this database, and some of these topics definitely needs more write-up:
 - buffer pool (instead of relying on the filesystem cache, I made my own LRU cache) `buffer_pool.rs`
 - heap (the original design had no intention for variable-length data, and adding a heap was a massive refactoring effort) `db1_string.rs`
 - Rust - Python FFI and Rust - C FFI (how to transfer data from a non-GC'ed language to a GC'ed language with minimal copies) `python_lib.rs`, `c_lib.rs`


The updated demo (video at the top) uses this database to load the full contents when the user clicks on a Wikibooks entry.

## Serialization/Deserialization

I considered using a well-established KV store, like LMDB, for the storage layer of the engine. I could build the inverted index without worrying about database organization and serialization. I would also get some guarantees, like thread-safety. I would also get features like prefix compression, cross-language FFI bindings, and a place to store metadata for free. However, I decided to roll my own storage engine and implement all serialization/deserialization code from scratch, based off the SSTable architecture. I went this route because storage architecture interested me as much as search engine architecture. Of course, if I was building this search engine for a business, rather than for my own curiosity, I would've chosen to use LMDB instead.

### VarInt
The first problem I encountered was storing fixed-with 32 bit integers. They were simple to serialize, but they wasted space. Most of the integers I needed to serialize (e.g. position offsets, document ID's, term frequencies) were in the 16 bit range. I took advantage of this by designing my own variable-int encoding. 

Numbers can be encoded in 1 byte, 2 bytes, 4 bytes, or 8 bytes. Smaller numbers require fewer bytes. The position of the [first set bit](https://man7.org/linux/man-pages/man3/ffs.3.html) determines the number of bytes the number has. I think this scheme is better, for my purposes, than [Protobuf's encoding](https://developers.google.com/protocol-buffers/docs/encoding#varints) because it requires at most 1 conditional branch and 2 read calls per number, whereas Protobuf is unbounded in branches/read calls. 

#### Problems with VarInt
As the indices grew bigger, decoding varints became the bottleneck. Therefore, I enabled "bit padding", which packs varints to 32 bits. The change in index size was negligible because I only enabled bit padding for a small subset of performance-sensitive regions in the index file. This padding let me read many numbers at once and deserialize them all in memory using SIMD instructions. In addition, I could read "padded" integers as a normal varint (without knowing beforehand it is padded) without issue. Thus, adding the padded integer feature did not break existing code at all.

## Sorted Set Intersection
In inverted-index based search engines, postings lists for query terms are intersected to find documents that contain ALL the terms ([deeper explanation here](https://nlp.stanford.edu/IR-book/html/htmledition/processing-boolean-queries-1.html)). This intersection soon became the bottleneck. For common terms like "begin," postings lists can surpass 100,000 documents. Even with sorting, galloping search, and SIMD optimizations, it was still too slow. 

Fortunately, I came across [tiered indexes](https://nlp.stanford.edu/IR-book/html/htmledition/tiered-indexes-1.html). This led to large performance improvements. At this point, I also implemented integer packing. Previously, I serialized postings list as:

`[number of postings: n]. Then "n" instances of (document_id: i32, document_frequency: i32).`

I changed the format to:

`[number of postings: n]. Then "n" instances of document_id: i32. Then "n" instances of document_frequency: i32.`

In other words, rather than storing tuples, I stored document_ids as their own block, then document_frequencies as their own block. This made delta-compression more efficient. In addition, should I decide to use an external compression algorithm in the future, this new format would increase the compressibility of my index.


## Search architecture
The by-default sharding of the database index into multiple, independent indices allows for very easy multi-threaded querying. Each index has a single-function API that inputs a vector of search terms and outputs a vector of matching filenames and their scores. Each index can independently choose whether to do position-based rescoring, prefix-query, and control its own implementation details. 

#### Ease of multithreading
Suppose we have 8 threads and 40 sharded indices. Each thread will handle 40/8 = 5 indices. Each thread will sequentially call each index. Each thread has 5 lists of filenames and their scores. Each thread will merge these lists into one master list, and return the top `n` documents from that list to a single master-thread. 

Thus, we now have 8 lists with `n` top documents each. We apply the merge again at the master thread, and take the top `n` documents from the merged list.

We could also make the database fully distributed across multiple servers. Each server would return the list of `n` top documents, and a similar merge step would be applied.


