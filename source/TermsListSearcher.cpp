// Initiated once for every term.
// Responsible for searching through that term's documents + frequencies list.
// Handles tiered-index searching as well.
// Tiered indexes: documents are split into document groups of 128 documents each. Total documents list is sorted
// by frequency, but within a document group, documents are sorted by score.


