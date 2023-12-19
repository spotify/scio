/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.smb

import org.apache.beam.sdk.extensions.{smb => beam}
import com.spotify.scio.{smb => scio}

import scala.jdk.CollectionConverters._

object SortedBucketIOUtil {
  def testId(read: beam.SortedBucketIO.Read[_]): String =
    scio.SortedBucketIO.testId(
      read
        .toBucketedInput(SortedBucketSource.Keying.PRIMARY)
        .getInputs
        .asScala
        .toSeq
        .map { case (rId, _) =>
          s"${rId.getCurrentDirectory}${Option(rId.getFilename).getOrElse("")}"
        }: _*
    )

  def testId(write: beam.SortedBucketIO.Write[_, _, _]): String =
    scio.SortedBucketIO.testId(write.getOutputDirectory.toString)

  def testId(write: beam.SortedBucketIO.TransformOutput[_, _, _]): String =
    scio.SortedBucketIO.testId(write.getOutputDirectory.toString)
}
