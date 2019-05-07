# Data Guidelines

Here are some common guidelines for building efficient, cost-effective, and maintainable pipelines. They apply to most use cases but you might have to tweak based on your needs. Also see the [[FAQ]] page for some common performance issues and remedies.

## Development

* Use Scio REPL to get familiar with Scio and perform ad-hoc experiments, but don't use it as a replacement for unit tests.

## Storage

* Leverage BigQuery, especially BigQuery SELECT query as input whenever possible. BigQuery has a very efficient columnar storage engine, can scale independently from Scio/Dataflow clusters and probably cheaper and easier to write than handcrafted Scala/Java pipeline code.
* Use BigQuery as an intermediate storage, especially if downstream jobs do a lot of slicing and dicing on rows and columns. Feel free to de-normalize data and use wide rows.
* Use Bigtable or Datastore depending on @ref[requirements](io/Bigtable.md#bigtable-vs-datastore) for serving pipeline output to production services.

## Computation

* Prefer combine/aggregate/reduce transforms over groupByKey. Keep in mind that a reduce operation must be [associative](https://en.wikipedia.org/wiki/Associative_property) and [commutative](https://en.wikipedia.org/wiki/Commutative_property).
* Prefer `sum`/`sumByKey` over `reduce`/`reduceByKey` for basic data types. They use [Algebird](https://github.com/twitter/algebird) `Semigroup`s and are often more optimized than hard coded reduce functions. See @github[AlgebirdSpec.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/AlgebirdSpec.scala) for more examples.
* Understand the performance characteristics of different types of joins and the role of side input cache, see the [[FAQ]] for more.
* Understand the Kryo serialization tuning options and use custom Kryo serializers for objects in the critical pass if necessary.

## Execution parameters

* When tuning [pipeline execution parameters](https://cloud.google.com/dataflow/pipelines/specifying-exec-params), start with smaller [`workerMachineType`](https://cloud.google.com/compute/docs/machine-types) e.g. default n1-standard-1 to n1-standard-4, and reasonable `maxNumWorkers` that reflect your input size. Keep in mind that there might be limited availability of large GCE instances and more workers means higher shuffle cost.

## Streaming

* For streaming jobs with periodically updated state, i.e. log decoration with metadata, keep (and update) states in Bigtable, and do look ups from the streaming job (read more about @ref[Bigtable key structure](io/Bigtable.md#key-structure)). Also see @scaladoc[BigtableDoFn](com.spotify.scio.bigtable.BigtableDoFn) for an abstraction that handles asynchronous Bigtable requests.
* For streaming, larger worker machine types and SSD for [`workerDiskType`](https://cloud.google.com/compute/docs/reference/latest/diskTypes) might be more suitable. A typical job with 5 x n1-standard-4 and 100GB SSDs can handle ~30k peak events per second. Also see this article on [disk performance](https://cloud.google.com/compute/docs/disks/performance).
