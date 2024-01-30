package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro._
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport, AvroWriteSupport}

object FixLogicalTypeSuppliers {
  implicit val c: Coder[GenericRecord] = ???
  val sc = ScioContext()

  sc.parquetAvroFile[GenericRecord]("input")

  sc.parquetAvroFile[GenericRecord]("input", null, null)

  sc.parquetAvroFile[GenericRecord]("input", conf = ParquetConfiguration.of("foo" -> "bar"))

  sc.parquetAvroFile[GenericRecord]("input", null, null, ParquetConfiguration.of("foo" -> "bar"))

  val data: SCollection[GenericRecord] = ???
  data.saveAsParquetAvroFile("output")

  data.saveAsParquetAvroFile("output", conf = ParquetConfiguration.of("foo" -> "bar"))

  val conf = new Configuration()
  
  
  
  
  conf.setClass("someClass", classOf[String], classOf[CharSequence])
}
