# Scio

[![Continuous Integration](https://github.com/spotify/scio/actions/workflows/ci.yml/badge.svg)](https://github.com/spotify/scio/actions/workflows/ci.yml)
[![codecov.io](https://codecov.io/github/spotify/scio/coverage.svg?branch=master)](https://codecov.io/github/spotify/scio?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/scio.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/scio-core_2.12.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/scio-core_2.12)
[![Scaladoc](https://img.shields.io/badge/scaladoc-latest-blue.svg)](https://spotify.github.io/scio/api/com/spotify/scio/index.html)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-brightgreen.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

<img src="https://raw.github.com/spotify/scio/master/site/src/main/paradox/images/scio.png" alt="Scio Logo" width="250"/>

> Ecclesiastical Latin IPA: /ˈʃi.o/, [ˈʃiː.o], [ˈʃi.i̯o]
> Verb: I can, know, understand, have knowledge.

Scio is a Scala API for [Apache Beam](http://beam.incubator.apache.org/) and [Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) inspired by [Apache Spark](http://spark.apache.org/) and [Scalding](https://github.com/twitter/scalding).

# Features

- Scala API close to that of Spark and Scalding core APIs
- Unified batch and streaming programming model
- Fully managed service<sup>\*</sup>
- Integration with Google Cloud products: Cloud Storage, BigQuery, Pub/Sub, Datastore, Bigtable
- Avro, Cassandra, Elasticsearch, gRPC, JDBC, neo4j, Parquet, Redis, [TensorFlow](http://tensorflow.org/) IOs
- Interactive mode with Scio REPL
- Type safe BigQuery
- Integration with [Algebird](https://github.com/twitter/algebird) and [Breeze](https://github.com/scalanlp/breeze)
- Pipeline orchestration with [Scala Futures](http://docs.scala-lang.org/overviews/core/futures.html)
- Distributed cache

<sup>\*</sup> provided by Google Cloud Dataflow

# Quick Start

Download and install the [Java Development Kit (JDK)](https://adoptopenjdk.net/index.html) version 8.

Install [sbt](https://www.scala-sbt.org/1.x/docs/Setup.html).

Use our [giter8 template](https://github.com/spotify/scio.g8) to quickly create a new Scio job repository:

`sbt new spotify/scio.g8`

Switch to the new repo (default `scio-job`) and build it:

```
cd scio-job
sbt stage
```

Run the included word count example:

`target/universal/stage/bin/scio-job --output=wc`

List result files and inspect content:

```
ls -l wc
cat wc/part-00000-of-00004.txt
```

# Documentation

[Getting Started](https://spotify.github.io/scio/Getting-Started.html) is the best place to start with Scio. If you are new to Apache Beam and distributed data processing, check out the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) first for a detailed explanation of the Beam programming model and concepts. If you have experience with other Scala data processing libraries, check out this comparison between [Scio, Scalding and Spark](https://spotify.github.io/scio/Scio,-Scalding-and-Spark.html).

Example Scio pipelines and tests can be found under [scio-examples](https://github.com/spotify/scio/tree/master/scio-examples/src). A lot of them are direct ports from Beam's Java [examples](https://github.com/apache/beam/tree/master/examples). See this [page](http://spotify.github.io/scio/examples/) for some of them with side-by-side explanation. Also see [Big Data Rosetta Code](https://github.com/spotify/big-data-rosetta-code) for common data processing code snippets in Scio, Scalding and Spark.

- [Scio Docs](https://spotify.github.io/scio/) - main documentation site
- [Scio Scaladocs](http://spotify.github.io/scio/api/) - current API documentation
- [Scio Examples](http://spotify.github.io/scio/examples/) - examples with side-by-side explanation

# Artifacts

Scio includes the following artifacts:

- `scio-avro`: add-on for Avro, can also be used standalone
- `scio-cassandra*`: add-ons for Cassandra
- `scio-core`: core library
- `scio-elasticsearch*`: add-ons for Elasticsearch
- `scio-extra`: extra utilities for working with collections, Breeze, etc., best effort support
- `scio-google-cloud-platform`: add-on for Google Cloud IO's: BigQuery, Bigtable, Pub/Sub, Datastore, Spanner
- `scio-grpc`: add-on for gRPC service calls
- `scio-jdbc`: add-on for JDBC IO
- `scio-neo4j`: add-on for Neo4J IO
- `scio-parquet`: add-on for Parquet
- `scio-redis`: add-on for Redis
- `scio-repl`: extension of the Scala REPL with Scio specific operations
- `scio-smb`: add-on for Sort Merge Bucket operations
- `scio-snowflake`: add-on for Snowflake IO
- `scio-tensorflow`: add-on for TensorFlow TFRecords IO and prediction
- `scio-test`: all following test utilities. Add to your project as a "test" dependency
  - `scio-test-core`: test core utilities
  - `scio-test-google-cloud-platform`: test utilities for Google Cloud IO's
  - `scio-test-parquet`: test utilities for Parquet

# License

Copyright 2024 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
