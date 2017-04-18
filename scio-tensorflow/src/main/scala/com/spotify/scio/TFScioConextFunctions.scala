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

package com.spotify.scio

import com.spotify.scio.io.{TFRecordOptions, TFRecordSource}
import com.spotify.scio.testing.TFRecordIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{io => gio}

class TFScioConextFunctions(val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a TensorFlow TFRecord file. Note that TFRecord files are not
   * splittable. The recommended record encoding is [[org.tensorflow.example.Example]] protocol
   * buffers (which contain [[org.tensorflow.example.Features]] as a field) serialized as bytes.
   * @group input
   */
  def tfRecordFile(path: String, tfRecordOptions: TFRecordOptions = TFRecordOptions.readDefault)
  : SCollection[Array[Byte]] = self.requireNotClosed {
    if (self.isTest) {
      self.getTestInput(TFRecordIO(path))
    } else {
      self.wrap(self.applyInternal(gio.Read.from(TFRecordSource(path, tfRecordOptions))))
    }
  }

}
