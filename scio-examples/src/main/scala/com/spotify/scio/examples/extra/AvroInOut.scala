/*
 * Copyright 2016 Spotify AB.
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

// Example: Avro Input and Output
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.avro._

object AvroInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Open Avro files as a `SCollection[TestRecord]` where `TestRecord` is an Avro specific record
    // Java class compiled from Avro schema.
    sc.avroFile[TestRecord](args("input"))
      .map { r =>
        // Create a new `Account` Avro specific record. It is recommended to use the builder over
        // constructor since it's more backwards compatible.
        Account.newBuilder()
          .setId(r.getIntField)
          .setType("checking")
          .setName(r.getStringField)
          .setAmount(r.getDoubleField)
          .build()
      }
      // Save result as Avro files
      .saveAsAvroFile(args("output"))

    // Close the context and execute the pipeline
    sc.close()
  }
}
