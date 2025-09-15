package fix.v0_13_0

import com.spotify.scio.avro.{GenericRecordParseTap, GenericRecordTap, ObjectFileTap, SpecificRecordTap}
import com.spotify.scio.io.TextTap
import com.spotify.scio.parquet.types.ParquetTypeIO
import com.spotify.scio.tensorflow.TFRecordFileTap
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import com.spotify.scio.avro.AvroIO
import com.spotify.scio.io.TextIO
import com.spotify.scio.tensorflow.TFRecordIO

object FixTaps {
  type T = SpecificRecord
  val path: String = ???
  SpecificRecordTap[T](path, AvroIO.ReadParam())

  ObjectFileTap[T](path, AvroIO.ReadParam())

  val schema: Schema = ???
  GenericRecordTap(path, schema, AvroIO.ReadParam())

  val parseFn: GenericRecord => T = ???
  GenericRecordParseTap(path, parseFn, AvroIO.ReadParam())

  TextTap(path, TextIO.ReadParam())

  ParquetTypeIO.ReadParam()

  TFRecordFileTap(path, TFRecordIO.ReadParam())
}
