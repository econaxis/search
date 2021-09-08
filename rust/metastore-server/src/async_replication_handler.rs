// Should export an impl of DatabaseInterface that broadcasts read/write requests to replicators
// Since all transport is async, we should send out all requests and return success once a majority of nodes
// confirm receipt of the message.

// Should also probably handle two-phase commit.