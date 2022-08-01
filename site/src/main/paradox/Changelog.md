# Changelog

## Important changes in 0.11.3
- Fixed a severe Parquet IO issue introduced in 0.11.2. Incompatible versions of `com.google.http-client:google-http-client:1.40.0` and `com.google.cloud.bigdataoss:gcsio:2.2.2` were leading to jobs reading Parquet getting stuck. The mitigation for 0.11.2 is to pin `google-http-client` to `1.39.2` in your build.sbt:
  ```scala
    dependencyOverrides ++= Seq(
    "com.google.http-client" % "google-http-client" % "1.39.2"
    )
  ```

## Breaking changes since Scio 0.10.0 (@ref:[v0.10.0 Migration Guide](migrations/v0.10.0-Migration-Guide.md))
- Move GCP modules to `scio-google-cloud-platform`
- Simplify coder implicits

## Breaking changes since Scio 0.9.0 (@ref:[v0.9.0 Migration Guide](migrations/v0.9.0-Migration-Guide.md))
- Drop Scala 2.11, add Scala 2.13 support
- Remove deprecated modules `scio-cassandra2` and `scio-elasticsearch2`
- Remove deprecated methods since 0.8.0
- Switch from Algebird `Hash128[K]` to Guava `Funnel[K]` for Bloom filter and sparse transforms

## Breaking changes since Scio 0.8.0 (@ref:[v0.8.0 Migration Guide](migrations/v0.8.0-Migration-Guide.md))
- `ScioIO`s no longer return `Future`
- `ScioContext#close` returns `ScioExecutionContext` instead of `ScioResult`
- Async `DoFn` refactor
- Deprecate `scio-cassandra2` and `scio-elasticsearch2`
- `ContextAndArgs#typed` no longer accepts list-case #2221

## Breaking changes since Scio 0.7.0 (@ref:[v0.7.0 Migration Guide](migrations/v0.7.0-Migration-Guide.md))

- New [Magnolia](https://github.com/softwaremill/magnolia) based @ref:[Coders](internals/Coders.md) derivation
- New @ref:[ScioIO](internals/ScioIO.md) replaces `TestIO[T]` to simplify IO implementation and stubbing in `JobTest`

## Breaking changes since Scio 0.6.0

- `scio-cassandra2` now requires Cassandra 2.2 instead of 2.0

## Breaking changes since Scio 0.5.0

- `BigQueryIO` in `JobTest` now requires a type parameter which could be either `TableRow` for JSON or `T` for type-safe API where `T` is a type annotated with `@BigQueryType`. Explicit `.map(T.toTableRow)` of test data is no longer needed. See changes in [BigQueryTornadoesTest](https://github.com/spotify/scio/commit/6ded455ba7506e619c484a05db5746cbee6d4dcd#diff-0d0a594c72b702523d4ad2e740253dcc) and [TypedBigQueryTornadoesTest](https://github.com/spotify/scio/commit/6ded455ba7506e619c484a05db5746cbee6d4dcd?diff=split#diff-0aae85e1d761a72c5ab1587fcc797b12) for more.
- Typed `AvroIO` now accepts case classes instead of Avro records in `JobTest`. Explicit `.map(T.toGenericRecord)` of test data is no longer needed. See this @github[change](19fee4716f71827ac4affbd23d753bc074c529b8) for more.
- Package `com.spotify.scio.extra.transforms` is moved from `scio-extra` to `scio-core`, under `com.spotify.scio.transforms`.

## Breaking changes since Scio 0.4.0

- Accumulators are replaced by the new metrics API, see @extref[MetricsExample.scala](example:MetricsExample) for more
- `com.spotify.scio.hdfs` package and related APIs (`ScioContext#hdfs*`, `SCollection#saveAsHdfs*`) are removed, regular file IO API should now support both GCS and HDFS (if `scio-hdfs` is included as a dependency).
- Starting Scio 0.4.4, Beam runner is completely decoupled from `scio-core`. See [[Runners]] page for more details.

## Breaking changes since Scio 0.3.0

- See this [page](https://cloud.google.com/dataflow/release-notes/release-notes-java-2) for a list of breaking changes from Dataflow Java SDK to Beam
- Scala 2.10 is dropped, 2.11 and 2.12 are the supported Scala binary versions
- Java 7 is dropped and Java 8+ is required
- `DataflowPipelineRunner` is renamed to `DataflowRunner`
- `DirectPipelineRunner` is renamed to `DirectRunner`
- `BlockingDataflowPipelineRunner` is removed and `ScioContext#close()` will not block execution; use `sc.run().waitUntilDone()` to retain the blocking behavior, i.e. if you launch job from an orchestration engine like [Airflow](https://airflow.apache.org/) or [Luigi](https://github.com/spotify/luigi)
- You should set `tempLocation` instead of `stagingLocation` regardless of runner; set it to a local path for `DirectRunner` or a GCS path for `DataflowRunner`; if not set, `DataflowRunner` will create a default bucket for the project
- Type safe BigQuery is now stable API; use `import com.spotify.scio.bigquery._` instead of `import com.spotify.scio.experimental._`
- `scio-bigtable` no longer depends on HBase and uses Protobuf based Bigtable API; check out the updated @extref[example](example:BigtableExample)
- Custom IO, i.e. `ScioContext#customInput` and `SCollection#saveAsCustomOutput` require a `name: String` parameter
