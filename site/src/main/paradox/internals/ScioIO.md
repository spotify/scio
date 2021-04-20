# ScioIO

Scio `0.7.0` introduces a new @scaladoc[`ScioIO[T]`](com.spotify.scio.io.ScioIO) trait to simplify IO implementation and stubbing in `JobTest`. This page lists some major changes to this new API.

## Dependencies

Avro and BigQuery logic was decoupled from `scio-core` as part of the refactor.

- Before `0.7.0`
  - `scio-core` depends on `scio-avro` and `scio-bigquery`
  - `ScioContext` and `SCollection[T]` include Avro, object, Protobuf and BigQuery IO methods out of the box
- After `0.7.0`
  - `scio-core` no longer depends on `scio-avro` and `scio-bigquery`
  - Import `com.spotify.scio.avro._` to get Avro, object, Protobuf IO methods on `ScioContext` and `SCollection[T]`
  - Import `com.spotify.scio.bigquery._` to get BigQuery IO methods on `ScioContext` and `SCollection[T]`
 
## `ScioIO[T]` for `JobTest`

As part of the refactor `TestIO[T]` was replaced by `ScioIO[T]` for `JobTest`. Some of them were moved to different packages for consistency but most test code should work with minor `import` changes. Below is a list of `ScioIO[T]` implementations.

- `com.spotify.scio.avro`
  - `AvroIO[T]`
  - `ObjectFileIO[T]`
  - `ProtobufIO[T]`
- `com.spotify.scio.bigquery`
  - `BigQueryIO[T]`
  - `TableRowJsonIO` where `T =:= TableRow`
- `com.spotify.scio.io`
  - `DatastoreIO` where `T =:= Entity`
  - `PubsubIO[T]`
  - `TextIO` where `T =:= String`
  - `CustomIO[T]` for use with `ScioContext#customInput` and `SCollection#customOutput`
- `com.spotify.scio.bigtable`
  - `BigtableIO[T]` where `T =:= Row` for input and `T =:= Mutation` for output
  - This replaces `BigtableInput` and `BigtableOutput`
- `com.spotify.scio.cassandra`
  - `CassandraIO[T]`
- `com.spotify.scio.elasticsearch`
  - `ElasticsearchIO[T]`
- `com.spotify.scio.extra.json`
  - `JsonIO[T]`
- `com.spotify.scio.jdbc`
  - `JdbcIO[T]`
- `com.spotify.scio.parquet.avro`
  - `ParquetAvroIO[T]`
- `com.spotify.scio.spanner`
  - `SpannerIO[T]`
- `com.spotify.scio.tensorflow`
  - `TFRecordIO` where `T =:= Array[Byte]`
  - `TFExampleIO` where `T =:= Example`

## Using `ScioIO[T]` directly

2 methods, `ScioContext#read` and `SCollection#write` were added to leverage `ScioIO[T]` directly without needing the extra `ScioContext#{textFile,AvroFile,...}` and `SCollection#saveAs{TextFile,AvroFile,...}` syntactic sugar. See @extref[WordCountScioIO](example:WordCountScioIO) and @github[WordCountScioIOTest](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/WordCountScioIOTest.scala) for concrete examples.
