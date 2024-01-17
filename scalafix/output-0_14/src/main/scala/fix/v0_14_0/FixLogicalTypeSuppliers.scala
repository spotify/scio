package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro._
import com.spotify.scio.values.SCollection
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.extensions.smb.AvroLogicalTypeSupplier
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport, AvroWriteSupport}

object FixLogicalTypeSuppliers {
  val sc = ScioContext()

  sc.parquetAvroFile[SpecificRecordBase]("input")

  sc.parquetAvroFile[SpecificRecordBase]("input", null, null)

  sc.parquetAvroFile[SpecificRecordBase]("input", conf = ParquetConfiguration.of("foo" -> "bar"))

  sc.parquetAvroFile[SpecificRecordBase]("input", null, null, ParquetConfiguration.of("foo" -> "bar"))

  val data: SCollection[SpecificRecordBase] = ???
  data.saveAsParquetAvroFile("output")

  data.saveAsParquetAvroFile("output", conf = ParquetConfiguration.of("foo" -> "bar"))

  val conf = new Configuration()
  conf.setClass("someClass", classOf[String], classOf[CharSequence])
}
