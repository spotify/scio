package com.spotify.scio.parquet.read

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.parquet.types._
import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.util.UUID

case class Record(strField: String)

class ParquetReadFnTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val records = (1 to 500).map(_ => Record(UUID.randomUUID().toString)).toList

  private val directory = {
    val d = Files.createTempDirectory("parquet")
    d.toFile.deleteOnExit()
    d.toString
  }

  override def beforeAll(): Unit = {
    // Multiple row-groups
    val multiRowGroupConf = new Configuration()
    multiRowGroupConf.setInt("parquet.block.size", 16)

    // Single row-group
    val singleRowGroupConf = new Configuration()
    singleRowGroupConf.setInt("parquet.block.size", 1073741824)

    val sc = ScioContext()
    val data = sc.parallelize(records)

    data.saveAsTypedParquetFile(s"$directory/multi", conf = multiRowGroupConf)
    data.saveAsTypedParquetFile(s"$directory/single", conf = singleRowGroupConf)

    sc.run()
  }

  "Parquet ReadFn" should "read at file-level granularity for files with multiple row groups" in {
    val granularityConf = new Configuration()
    granularityConf.set(
      ParquetReadConfiguration.SplitGranularity,
      ParquetReadConfiguration.SplitGranularityFile
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](s"$directory/multi/*.parquet", conf = granularityConf)
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }

  it should "read at file-level granularity for files with a single row group" in {
    val granularityConf = new Configuration()
    granularityConf.set(
      ParquetReadConfiguration.SplitGranularity,
      ParquetReadConfiguration.SplitGranularityFile
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](s"$directory/single/*.parquet", conf = granularityConf)
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }

  it should "read at row-group granularity for files with multiple row groups" in {
    val granularityConf = new Configuration()
    granularityConf.set(
      ParquetReadConfiguration.SplitGranularity,
      ParquetReadConfiguration.SplitGranularityRowGroup
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](s"$directory/multi/*.parquet", conf = granularityConf)
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }

  it should "read at row-group granularity for files with a single row groups" in {
    val granularityConf = new Configuration()
    granularityConf.set(
      ParquetReadConfiguration.SplitGranularity,
      ParquetReadConfiguration.SplitGranularityRowGroup
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](s"$directory/single/*.parquet", conf = granularityConf)
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }
}
