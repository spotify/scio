/*
rule = FixTaps
*/
package fix.v0_13_0

import com.spotify.scio.avro.{GenericRecordParseTap, GenericRecordTap, ObjectFileTap, SpecificRecordTap}
import com.spotify.scio.io.TextTap
import com.spotify.scio.parquet.types.ParquetTypeIO
import com.spotify.scio.tensorflow.TFRecordFileTap
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

object FixTaps {
  type T = SpecificRecord
  val path: String = ???
  SpecificRecordTap[T](path)

  ObjectFileTap[T](path)

  val schema: Schema = ???
  GenericRecordTap(path, schema)

  val parseFn: GenericRecord => T = ???
  GenericRecordParseTap(path, parseFn)

  TextTap(path)

  ParquetTypeIO.ReadParam[T]()

  TFRecordFileTap(path)
}
