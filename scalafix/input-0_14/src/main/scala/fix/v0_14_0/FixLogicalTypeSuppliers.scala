/*
rule = FixLogicalTypeSupplier
 */
package fix.v0_14_0

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.avro.LogicalTypeSupplier
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.smb.AvroLogicalTypeSupplier
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport, AvroWriteSupport}

object FixLogicalTypeSuppliers {
  implicit val c: Coder[GenericRecord] = ???
  val sc = ScioContext()

  sc.parquetAvroFile[GenericRecord](
    "input",
    conf = ParquetConfiguration.of(
      AvroReadSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier]
    ))

  sc.parquetAvroFile[GenericRecord](
    "input",
    null,
    null,
    ParquetConfiguration.of(
      (AvroReadSupport.AVRO_DATA_SUPPLIER, classOf[LogicalTypeSupplier])
    ))

  sc.parquetAvroFile[GenericRecord](
    "input",
    conf = ParquetConfiguration.of(
      AvroReadSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier],
      "foo" -> "bar"
    ))

  sc.parquetAvroFile[GenericRecord](
    "input",
    null,
    null,
    ParquetConfiguration.of(
      AvroReadSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier],
      "foo" -> "bar"
    ))

  val data: SCollection[GenericRecord] = ???
  data.saveAsParquetAvroFile(
    "output",
    conf = ParquetConfiguration.of(
      AvroWriteSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier]
    )
  )

  data.saveAsParquetAvroFile(
    "output",
    conf = ParquetConfiguration.of(
      AvroWriteSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier],
      "foo" -> "bar"
    )
  )

  val conf = new Configuration()
  conf.setClass(AvroReadSupport.AVRO_DATA_SUPPLIER, classOf[LogicalTypeSupplier], classOf[AvroDataSupplier])
  conf.setClass(AvroWriteSupport.AVRO_DATA_SUPPLIER, classOf[LogicalTypeSupplier], classOf[LogicalTypeSupplier])
  conf.setClass(AvroReadSupport.AVRO_DATA_SUPPLIER, classOf[AvroLogicalTypeSupplier], classOf[AvroDataSupplier])
  conf.setClass(AvroWriteSupport.AVRO_DATA_SUPPLIER, classOf[AvroLogicalTypeSupplier], classOf[LogicalTypeSupplier])
  conf.setClass("someClass", classOf[String], classOf[CharSequence])
}
