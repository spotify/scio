package com.spotify.scio.snowflake

import com.spotify.scio.testing.ScioIOSpec
import kantan.csv.RowCodec

object SnowflakeIOTest {
  final case class Data(value: String)
}

class SnowflakeIOTest extends ScioIOSpec {

  import SnowflakeIOTest._

  val connectionOptions = SnowflakeConnectionOptions(
    url = "jdbc:snowflake://host.snowflakecomputing.com"
  )

  implicit val rowCodecData: RowCodec[Data] = RowCodec.caseCodec(Data.apply)(Data.unapply)

  "SnowflakeIO" should "support query input" in {
    val input = Seq(Data("a"), Data("b"), Data("c"))
    val query = "SELECT * FROM table"
    testJobTestInput(input, query)(SnowflakeIO(connectionOptions, _))(
      _.snowflakeQuery(connectionOptions, _, "storage-integration")
    )
  }

  it should "support table input" in {
    val input = Seq(Data("a"), Data("b"), Data("c"))
    val table = "table"
    testJobTestInput(input, table)(SnowflakeIO(connectionOptions, _))(
      _.snowflakeTable(connectionOptions, _, "storage-integration")
    )
  }

  it should "support table output" in {
    val output = Seq(Data("a"), Data("b"), Data("c"))
    val table = "table"
    testJobTestOutput(output, table)(SnowflakeIO(connectionOptions, _))(
      _.saveAsSnowflake(connectionOptions, _)
    )
  }
}
