/*
 * Copyright 2019 Spotify AB.
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

// Example: Read and Write specific and generic Avro records
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.ParquetExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=[INPUT].parquet --output=[OUTPUT].parquet --method=[METHOD]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.avro.Account
import com.spotify.scio.io.ClosedTap
import org.apache.hadoop.conf.Configuration
import org.apache.avro.generic.GenericRecord

object ParquetExample {

  case class AccountInput(id: Int, `type`: String, name: String, amount: Double)

  case class AccountOutput(id: Int, name: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val m = args("method")
    m match {
      // Read Parquet files as Specific Avro Records
      case "avroSpecific" => avroSpecific(sc, args)

      // Read Parquet files as Generic Avro Records
      case "avroGeneric" => avroGeneric(sc, args)

      // Read Parquet files as Scala Case Classes
      case "caseClass" => caseClass(sc, args)

      case _ => throw new RuntimeException(s"Invalid method $m")
    }

    sc.run()
    ()
  }

  private def avroSpecific(sc: ScioContext, args: Args): ClosedTap[AccountOutput] = {

    // Account is an Avro Specific record. The avsc schema for the same can be found in scio-schemas/src/main/avro/schema.avsc
    // Macros for generating column projections and row predicates
    val projection =
      Projection[Account](_.getId, _.getName, _.getAmount) // Only these columns will be projected
    val predicate = Predicate.build[Account](x =>
      x.getAmount > 0
    ) // Will skip row groups where this column value check is not satisfied

    sc.parquetAvroFile[Account](args("input"), projection, predicate.parquet)
      // filter natively with the same logic in case of mock input in tests
      .filter(predicate.native)

      // The result Account records are not complete Avro objects. Only the projected columns are present while the rest are null.
      // These objects may fail serialization and it’s recommended that you map them out to tuples or case classes right after reading.
      .map(x => AccountOutput(x.getId(), x.getName().toString()))

      // This case class can now be saved as a parquet file
      // numShards should be explicitly set so that the size of each output file is smaller than
      // but close to `parquet.block.size`, i.e. 1 GiB. This guarantees that each file contains 1 row group only and reduces seeks.
      .saveAsTypedParquetFile(args("output"), numShards = 5, conf = fineTunedParquetWriterConfig())
  }

  private def avroGeneric(sc: ScioContext, args: Args): ClosedTap[AccountOutput] = {

    // Now the fields in Account's schema act as our projection
    sc.parquetAvroFile[GenericRecord](args("input"), Account.getClassSchema)

      // GenericRecord objects are also not Serializable.
      // Map out projected fields into something type safe
      .map(r => AccountOutput(r.get("id").asInstanceOf[Int], r.get("name").toString))
      // This case class can now be saved as a parquet file
      .saveAsTypedParquetFile(
        args("output")
      ) // passing writer configuration is optional but recommended
  }

  private def caseClass(sc: ScioContext, args: Args): ClosedTap[Account] = {

    // All fields in the case class definition act as our projection
    sc.typedParquetFile[AccountInput](args("input"))

      // We can also save the result as Parquet Avro files
      .map(x =>
        Account
          .newBuilder()
          .setId(x.id)
          .setType(x.`type`)
          .setName(x.name)
          .setAmount(x.amount)
          .build()
      )
      .saveAsParquetAvroFile(args("output"))

  }

  // See this link for parquet writer tuning https://spotify.github.io/scio/io/Parquet.html#performance-tuning
  private def fineTunedParquetWriterConfig(): Configuration = {
    val conf: Configuration = new Configuration()
    conf.setInt("parquet.block.size", 1073741824) // 1 * 1024 * 1024 * 1024 = 1 GiB
    conf.set("fs.gs.inputstream.fadvise", "RANDOM")
    conf
  }
}
