/*
 * Copyright 2021 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Example: Read and write Parquet in Avro and Typed formats
// Usage:
// `sbt "runMain com.spotify.scio.examples.extra.ParquetExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION]
// --input=[INPUT]/*.parquet --output=[OUTPUT] --method=[METHOD]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.avro.{Account, AccountStatus}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import org.apache.hadoop.conf.Configuration
import org.apache.avro.generic.GenericRecord

object ParquetExample {

  /**
   * These case classes represent both full and projected field mappings from the [[Account]] Avro
   * record.
   */
  case class AccountFull(id: Int, `type`: String, name: Option[String], amount: Double)
  case class AccountProjection(id: Int, name: Option[String])

  /**
   * A Hadoop [[Configuration]] can optionally be passed for Parquet reads and writes to improve
   * performance.
   *
   * See more here: https://spotify.github.io/scio/io/Parquet.html#performance-tuning
   */
  private val fineTunedParquetWriterConfig: Configuration = {
    val conf: Configuration = new Configuration()
    conf.setInt("parquet.block.size", 1073741824) // 1 * 1024 * 1024 * 1024 = 1 GiB
    conf.set("fs.gs.inputstream.fadvise", "RANDOM")
    conf
  }

  private[extra] val fakeData: Iterable[Account] =
    (1 to 100)
      .map(i =>
        Account
          .newBuilder()
          .setId(i)
          .setType(if (i % 3 == 0) "current" else "checking")
          .setName(s"account $i")
          .setAmount(i.toDouble)
          .setAccountStatus(if (i % 2 == 0) AccountStatus.Active else AccountStatus.Inactive)
          .build()
      )

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val m = args("method")
    m match {
      // Read Parquet files as Specific Avro Records
      case "avroSpecificIn" => avroSpecificIn(sc, args)

      // Read Parquet files as Generic Avro Records
      case "avroGenericIn" => avroGenericIn(sc, args)

      // Read Parquet files as Scala Case Classes
      case "typedIn" => typedIn(sc, args)

      // Write dummy Parquet Avro records
      case "avroOut" => avroOut(sc, args)

      // Write dummy Parquet records using case classes
      case "typedOut" => typedOut(sc, args)

      case _ => throw new RuntimeException(s"Invalid method $m")
    }

    sc.run().waitUntilDone()
    ()
  }

  private def avroSpecificIn(sc: ScioContext, args: Args): ClosedTap[String] = {
    // Macros for generating column projections and row predicates
    val projection = Projection[Account](_.getId, _.getName, _.getAmount)
    val predicate = Predicate[Account](x => x.getAmount > 0)

    sc.parquetAvroFile[Account](args("input"), projection, predicate)
      // The result Account records are not complete Avro objects. Only the projected columns are present while the rest are null.
      // These objects may fail serialization and it’s recommended that you map them out to tuples or case classes right after reading.
      .map(x => AccountProjection(x.getId, Some(x.getName.toString)))
      .saveAsTextFile(args("output"))
  }

  private def avroGenericIn(sc: ScioContext, args: Args): ClosedTap[String] = {
    val schema = Account.getClassSchema
    implicit val genericRecordCoder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)

    val parquetIn = sc.parquetAvroFile[GenericRecord](args("input"), schema)

    // Catches a specific bug with encoding GenericRecords read by parquet-avro
    parquetIn
      .map(identity)
      .count

    // We can also pass an Avro schema directly to project into Avro GenericRecords.
    parquetIn
      // Map out projected fields into something type safe
      .map(r => AccountProjection(r.get("id").asInstanceOf[Int], Some(r.get("name").toString)))
      .saveAsTextFile(args("output"))
  }

  private def typedIn(sc: ScioContext, args: Args): ClosedTap[String] =
    sc.typedParquetFile[AccountProjection](args("input"))
      .saveAsTextFile(args("output"))

  private def avroOut(sc: ScioContext, args: Args): ClosedTap[Account] =
    sc.parallelize(fakeData)
      // numShards should be explicitly set so that the size of each output file is smaller than
      // but close to `parquet.block.size`, i.e. 1 GiB. This guarantees that each file contains 1 row group only and reduces seeks.
      .saveAsParquetAvroFile(args("output"), numShards = 1, conf = fineTunedParquetWriterConfig)

  private def typedOut(sc: ScioContext, args: Args): ClosedTap[AccountFull] =
    sc.parallelize(fakeData)
      .map(x => AccountFull(x.getId, x.getType.toString, Some(x.getName.toString), x.getAmount))
      .saveAsTypedParquetFile(
        args("output")
      )

}
