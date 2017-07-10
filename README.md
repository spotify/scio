Scio
====

[![Build Status](https://img.shields.io/circleci/project/github/spotify/scio/master.svg)](https://circleci.com/gh/spotify/scio)
[![codecov.io](https://codecov.io/github/spotify/scio/coverage.svg?branch=master)](https://codecov.io/github/spotify/scio?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/scio.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/scio-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/scio-core_2.11)
[![Join the chat at https://gitter.im/spotify/scio](https://badges.gitter.im/spotify/scio.svg)](https://gitter.im/spotify/scio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

> Ecclesiastical Latin IPA: /ˈʃi.o/, [ˈʃiː.o], [ˈʃi.i̯o]

> Verb: I can, know, understand, have knowledge.

Scio is a Scala API for [Apache Beam](http://beam.incubator.apache.org/) and [Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) inspired by [Apache Spark](http://spark.apache.org/) and [Scalding](https://github.com/twitter/scalding). See the [current API documentation](http://spotify.github.io/scio/) for more information.

Scio 0.3.0 and future versions depend on Apache Beam (`org.apache.beam`) while earlier versions depend on Google Cloud Dataflow SDK (`com.google.cloud.dataflow`). See this [page](https://github.com/spotify/scio/wiki/Apache-Beam) for a list of breaking changes.

# Features

- Scala API close to that of Spark and Scalding core APIs
- Unified batch and streaming programming model<sup>1, 2</sup>
- Fully managed service<sup>2</sup>
- Integration with Google Cloud products: Cloud Storage, BigQuery, Pub/Sub, Datastore, Bigtable<sup>2</sup>
- HDFS source/sink
- Interactive mode with Scio REPL
- Type safe BigQuery
- Integration with [Algebird](https://github.com/twitter/algebird) and [Breeze](https://github.com/scalanlp/breeze)
- Pipeline orchestration with [Scala Futures](http://docs.scala-lang.org/overviews/core/futures.html)
- Distributed cache

<sup>1</sup> provided by Apache Beam

<sup>2</sup> provided by Google Cloud Dataflow

# Quick Start

The ubiquitous word count example can be run directly with SBT in local mode, using `README.md` as input.

```bash
sbt "scio-examples/run-main com.spotify.scio.examples.WordCount --input=README.md --output=wc"
cat wc/part-00000-of-00001.txt
```

# Documentation

- [Scio Wiki](https://github.com/spotify/scio/wiki) - wiki page
- [ScalaDocs](http://spotify.github.com/scio) - current API documentation
- [Big Data Rosetta Code](https://github.com/spotify/big-data-rosetta-code) - comparison of code snippets in Scio, Scalding and Spark

# Artifacts

Scio includes the following artifacts:

- `scio-core`: core library
- `scio-test`: test utilities, add to your project as a "test" dependency
- `scio-bigquery`: add-on for BigQuery, included in `scio-core` but can also be used standalone
- `scio-bigtable`: add-on for Bigtable
- `scio-cassandra2`: add-on for Cassandra 2.x
- `scio-cassandra3`: add-on for Cassandra 3.x
- `scio-elasticsearch2`: add-on for Elasticsearch 2.x
- `scio-elasticsearch5`: add-on for Elasticsearch 5.x
- `scio-extra`: extra utilities for working with collections, Breeze, etc.
- `scio-hdfs`: add-on for HDFS IO
- `scio-jdbc`: add-on for JDBC IO
- `scio-tensorflow`: add-on for [TensorFlow](http://tensorflow.org/) TFRecords IO and prediction

# License

Copyright 2016 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
