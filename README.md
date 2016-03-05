Scio
====

[![Build Status](https://travis-ci.org/spotify/scio.svg?branch=master)](https://travis-ci.org/spotify/scio)
[![codecov.io](https://codecov.io/github/spotify/scio/coverage.svg?branch=master)](https://codecov.io/github/spotify/scio?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/scio.svg)]()
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/scio-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/scio-core_2.11)

_Ecclesiastical Latin IPA: /ˈʃi.o/, [ˈʃiː.o], [ˈʃi.i̯o]_

_Verb: I can, know, understand, have knowledge._

Scio is a Scala API for [Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/DataflowJavaSDK), inspired by [Spark](http://spark.apache.org/) and [Scalding](https://github.com/twitter/scalding). See the [current API documentation](http://spotify.github.io/scio/) for more information.

# Getting Started

First install the [Google Cloud SDK](https://cloud.google.com/sdk/) and create a [Google Cloud Storage](https://cloud.google.com/storage/) bucket for your project, e.g. `gs://my-bucket`. Make sure it's in the same region as the [BigQuery](https://cloud.google.com/bigquery/) datasets you want to access and where you want Dataflow to launch workers on GCE.

# Documentation
* [Getting Started](https://github.com/spotify/scio/wiki#running-the-examples)
* [Scaladocs](http://spotify.github.com/scio): Generated documentation for current version of Scio.
* [Scio REPL](https://github.com/spotify/scio/blob/master/scio-repl/README.md): Interactive learning. This tutorial shows off how to interact with scio via REPL.
* [BigQuery settings](https://github.com/spotify/scio/wiki#bigquery-settings): BigQuery settings cheat sheet.
* [Options](https://github.com/spotify/scio/wiki#options): Google Dataflow settings cheat sheet.
* [Scio, Spark and Scalding](https://github.com/spotify/scio/wiki#scio-spark-and-scalding)

# Building Scio

Scio is built using [SBT](http://www.scala-sbt.org/). To build Scio and publish artifacts locally, run:

```bash
git clone git@github.com:spotify/scio.git
cd scio
sbt publish-local
```

# Artifacts

Scio includes the following artifacts:

- `scio-core`: core library
- `scio-test`: test utilities, add to your project as a "test" dependency
- `scio-bigquery`: Add-on for BigQuery, included in `scio-core` but can also be used standalone
- `scio-bigtable`: Add-on for Bigtable
- `scio-extra`: Extra utilities for working with collections, Breeze, etc.
- `scio-hdfs`: Add-on for HDFS

To access HDFS from a Scio job, Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`, etc.) must be present in `src/main/resources` and `--network` should be set to one that has access to the Hadoop cluster.

# License

Copyright 2016 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
