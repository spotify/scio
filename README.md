Scio
====

[![Build Status](https://img.shields.io/circleci/project/github/spotify/scio/master.svg)](https://circleci.com/gh/spotify/scio)
[![codecov.io](https://codecov.io/github/spotify/scio/coverage.svg?branch=master)](https://codecov.io/github/spotify/scio?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/scio.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/scio-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/scio-core_2.11)
[![Join the chat at https://gitter.im/spotify/scio](https://badges.gitter.im/spotify/scio.svg)](https://gitter.im/spotify/scio)

![Scio Logo](https://raw.github.com/spotify/scio/master/logo/logo.png)

> Ecclesiastical Latin IPA: /ˈʃi.o/, [ˈʃiː.o], [ˈʃi.i̯o]

> Verb: I can, know, understand, have knowledge.

Scio is a Scala API for [Apache Beam](http://beam.incubator.apache.org/) and [Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) inspired by [Apache Spark](http://spark.apache.org/) and [Scalding](https://github.com/twitter/scalding).

Scio 0.3.0 and future versions depend on Apache Beam (`org.apache.beam`) while earlier versions depend on Google Cloud Dataflow SDK (`com.google.cloud.dataflow`). See this [page](https://github.com/spotify/scio/wiki/Apache-Beam) for a list of breaking changes.

# Features

- Scala API close to that of Spark and Scalding core APIs
- Unified batch and streaming programming model
- Fully managed service<sup>*</sup>
- Integration with Google Cloud products: Cloud Storage, BigQuery, Pub/Sub, Datastore, Bigtable
- HDFS, JDBC, [TensorFlow](http://tensorflow.org/) TFRecords, Cassandra, Elasticsearch and Parquet I/O
- Interactive mode with Scio REPL
- Type safe BigQuery
- Integration with [Algebird](https://github.com/twitter/algebird) and [Breeze](https://github.com/scalanlp/breeze)
- Pipeline orchestration with [Scala Futures](http://docs.scala-lang.org/overviews/core/futures.html)
- Distributed cache

<sup>*</sup> provided by Google Cloud Dataflow

# Quick Start

The ubiquitous word count example can be run directly with SBT in local mode, using `README.md` as input.

```bash
sbt "scio-examples/run-main com.spotify.scio.examples.WordCount --input=README.md --output=wc"
cat wc/part-00000-of-00001.txt
```

# Documentation

[Getting Started](https://github.com/spotify/scio/wiki/Getting-Started) is the best place to start with Scio. If you are new to Apache Beam and distributed data processing, check out the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) first for a detailed explanation of the Beam programming model and concepts. If you have experience with other Scala data processing libraries, check out this comparison between [Scio, Scalding and Spark](https://github.com/spotify/scio/wiki/Scio%2C-Scalding-and-Spark). Finally check out this document about the relationship between [Scio, Beam and Dataflow](https://github.com/spotify/scio/wiki/Scio,-Beam-and-Dataflow).

Example Scio pipelines and tests can be found under [scio-examples](https://github.com/spotify/scio/tree/master/scio-examples/src). A lot of them are direct ports from Beam's Java [examples](https://github.com/apache/beam/tree/master/examples). See this [page](http://spotify.github.io/scio/examples/) for some of them with side-by-side explanation. Also see [Big Data Rosetta Code](https://github.com/spotify/big-data-rosetta-code) for common data processing code snippets in Scio, Scalding and Spark.

- [Scio Wiki](https://github.com/spotify/scio/wiki) - wiki page
- [Scio Scaladocs](http://spotify.github.io/scio) - current API documentation
- [Scio Examples](http://spotify.github.io/scio/examples/) - examples with side-by-side explanation

# Artifacts

Scio includes the following artifacts:

- `scio-core`: core library
- `scio-test`: test utilities, add to your project as a "test" dependency
- `scio-avro`: add-on for Avro, included in `scio-core` but can also be used standalone
- `scio-bigquery`: add-on for BigQuery, included in `scio-core` but can also be used standalone
- `scio-bigtable`: add-on for Bigtable
- `scio-cassandra2`: add-on for Cassandra 2.x
- `scio-cassandra3`: add-on for Cassandra 3.x
- `scio-elasticsearch2`: add-on for Elasticsearch 2.x
- `scio-elasticsearch5`: add-on for Elasticsearch 5.x
- `scio-extra`: extra utilities for working with collections, Breeze, etc.
- `scio-hdfs`: add-on for HDFS IO
- `scio-jdbc`: add-on for JDBC IO
- `scio-parquet`: add-on for Parquet
- `scio-tensorflow`: add-on for TensorFlow TFRecords IO and prediction

# License

Copyright 2016 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
