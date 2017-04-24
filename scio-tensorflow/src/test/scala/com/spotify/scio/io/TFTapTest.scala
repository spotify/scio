/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.io

import java.util.UUID

import org.apache.commons.io.FileUtils

class TFTapTest extends TapSpec {

  "SCollection" should "support saveAsTFRecordFile" in {
    import com.spotify.scio.tensorflow._
    val data = Seq.fill(100)(UUID.randomUUID().toString)
    import TFRecordOptions.CompressionType._
    for (compressionType <- Seq(NONE, ZLIB, GZIP)) {
      val dir = tmpDir
      val t = runWithFileFuture {
        _
          .parallelize(data)
          .map(_.getBytes)
          .saveAsTfRecordFile(dir.getPath, tfRecordOptions = TFRecordOptions(compressionType))
      }
      verifyTap(t.map(new String(_)), data.toSet)
      FileUtils.deleteDirectory(dir)
    }
  }

}
