# Bigtable

First please read Google's official [doc](https://cloud.google.com/bigtable/docs/how-to).

### Bigtable example

This depends on APIs from `scio-bigtable` and imports from `com.spotify.scio.bigtable._`.

Look at example [here](https://github.com/spotify/scio/blob/master/scio-examples/src/main/scala/com/spotify/scio/examples/extra/BigtableExample.scala).

## Common issues

#### Size of the cluster vs Dataflow cluster

As a general note when writing to Bigtable from Dataflow you should at most use the # of Bigtable nodes you have * 3 cpus. Otherwise Bigtable will be overwhelmed and throttle the writes (and the reads)

#### Cell compression

Bigtable doesn't compress cell values > 1Mb

#### `Jetty ALPN/NPN has not been properly configured`

Check that your versions of `grpc-netty`, `netty-handler`, and `netty-tcnative-boringssl-static` are [compatible](https://github.com/grpc/grpc-java/blob/master/SECURITY.md#troubleshooting).

#### BigtableIO

The BigtableIO included in the Dataflow SDK is not recommended for use. It is not written by the Bigtable team and is significantly less performant than the HBase Bigtable Dataflow connector. Please see the example above for the recommended API.

#### Key structure

Your row key should not contain common parts at the beginning of the key, doing so would overload specific Bigtable nodes. For example, if your row is identifiable by `user-id` and `date` key - do NOT use `date,user-id`, instead use `user-id,date` or even better in case of `date` use Bigtable `version/timestamp`. Read more about row key design over [here](https://cloud.google.com/bigtable/docs/schema-design#row-keys).

#### Performance

Read Google [doc](https://cloud.google.com/bigtable/docs/performance).

### Bigtable vs Datastore

If you require replacement for Cassandra, Bigtable is probable the most straightforward replacement in GCP.
Bigtable [white paper](https://static.googleusercontent.com/media/research.google.com/en/archive/bigtable-osdi06.pdf). To quote the paper - think of Bigtable as:
> a sparse, distributed, persistent multidimensional sorted map. The map is indexed by a row key, column key, and a timestamp; each value in the map is an uninterpreted array of bytes.

Bigtable is replicated only within a single zone. Bigtable does not support transactions, that said all operations are atomic at the row level.

Think of Datastore as distributed, persistent, fully managed key-value store, with support for transactions. Datastore is replicated across multiple datacenters thus making it theoretically more available than Bigtable (as of today).

Read more about Bigtable [here](https://cloud.google.com/bigtable/docs/concepts), and more about Datastore over [here](https://cloud.google.com/datastore/docs/concepts/overview).
