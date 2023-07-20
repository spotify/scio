/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import java.nio.ByteBuffer
import com.spotify.scio.io.BinaryIO.BinaryFileReader
import java.io.InputStream

// Example: Binary Input and Output
// Usage:
// `sbt "runMain com.spotify.scio.examples.extra.BinaryInOut
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME] --output=[OUTPUT]"`

object BinaryInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val in = (1 to 10)
      .map(i => 0 to i)
      .map(_.toArray.map(_.toByte))

    def intToPaddedArray(i: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(i).array()

    sc.parallelize(in)
      .saveAsBinaryFile(
        args("output"),
        // Write each file with a static header ...
        header = Array[Byte](1, 2, 3),
        // a record prefix containing an integer record length ...
        framePrefix = arr => intToPaddedArray(arr.length),
        // and a single byte suffix.
        frameSuffix = _ => Array[Byte](0)
      )
    sc.run().waitUntilDone()

    val (sc2, _) = ContextAndArgs(cmdlineArgs)
    sc2
      // Read back the just-written data, parsing the written files with `MyBinaryFileReader`.
      .binaryFile(args("output"), MyBinaryFileReader)
      .map(_.mkString("Array(", ",", ")"))
      .debug()
    sc2.run().waitUntilDone()
  }

  // This BinaryFileReader subclass must match the behavior of the write.
  case object MyBinaryFileReader extends BinaryFileReader {
    // This example has no state, but a record header could for example contain a number of
    // expected records in the entire file or some block, and the state could then be used track
    // the number of records read and to determine when the reader would need to switch modes from
    // reading records to reading block metadata or the file footer.
    override type State = Unit

    private def fail(msg: String) = throw new IllegalStateException(msg)

    override def start(is: InputStream): State = {
      // Read the expected magic number from the first bytes of the file, and fail if it is not
      // found.
      val b = new Array[Byte](3)
      val readBytes = is.read(b)
      if (readBytes != b.length) fail("Failed to read header")

      val magicNumberOk = b(0) == 1 && b(1) == 2 && b(2) == 3
      if (!magicNumberOk) fail("Failed to find correct magic number")
      ()
    }

    override def readRecord(state: State, is: InputStream): (State, Array[Byte]) = {
      // Read the number of expected bytes for a record, corresponding to the `framePrefix`
      // argument.
      val sizeBuf = new Array[Byte](4)
      val sizeBytesRead = is.read(sizeBuf)
      // If the entire file has been consumed return `null` to indicate that the read is complete.
      // If a record count was maintained in `state`, then once all records were read a similar
      // value should be returned.
      if (sizeBytesRead == -1) (state, null)
      else {
        if (sizeBytesRead != 4) fail(s"Failed to read record size $sizeBytesRead")
        else {
          val size = ByteBuffer.wrap(sizeBuf).getInt
          if (size < 0) fail(s"Bad record size $size")
          val elementBytes = new Array[Byte](size)
          val bytesRead = is.read(elementBytes)
          // Ensure all expected bytes are read.
          if (bytesRead != size) fail("Failed to read expected record bytes")
          else {
            val suffix = is.read()
            // Ensure `frameSuffix` is read.
            if (suffix != 0) fail(s"Failed to read expected record suffix $suffix")
            // Return the record and state. If a record count was maintained in `state` it would
            // be incremented here.
            else (state, elementBytes)
          }
        }
      }
    }

    // There is no footer to read and no validation which needs to occur, so return Unit
    override def end(state: State, is: InputStream): Unit = ()
  }
}
