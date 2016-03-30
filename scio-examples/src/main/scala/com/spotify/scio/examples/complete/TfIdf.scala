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

package com.spotify.scio.examples.complete

import java.io.File
import java.net.URI

import com.google.cloud.dataflow.sdk.options.GcsOptions
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.SCollection

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.complete.TfIdf
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/shakespeare/?*.txt
  --output=gs://[BUCKET]/[PATH]/tf_idf
*/

object TfIdf {
  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val uris = {
      val base = new URI(args.getOrElse("input", ExampleData.SHAKESPEARE_PATH))
      val absoluteUri = if (base.getScheme != null) {
        base
      } else {
        new URI("file", base.getAuthority, base.getPath, base.getQuery, base.getFragment)
      }

      if (absoluteUri.getScheme == "file") {
        val dir = new File(absoluteUri)
        dir.list().map(e => new File(dir, e).toURI).toSet
      } else if (absoluteUri.getScheme == "gs") {
        val glob = new URI(
          absoluteUri.getScheme,
          absoluteUri.getAuthority,
          absoluteUri.getPath + "*",
          absoluteUri.getQuery,
          absoluteUri.getFragment)
        sc.options.asInstanceOf[GcsOptions].getGcsUtil
          .expand(GcsPath.fromUri(glob)).asScala.map(_.toUri).toSet
      } else {
        throw new IllegalArgumentException(s"Unsupported scheme ${absoluteUri.getScheme}")
      }
    }

    val uriToContent = SCollection.unionAll(uris.map { uri =>
      val uriString = if (uri.getScheme == "file") new File(uri).getPath else uri.toString
      sc.textFile(uriString).keyBy(_ => uriString)
    }.toSeq)

    val uriToWords = uriToContent.flatMap { case (uri, line) =>
      line.split("\\W+").filter(_.nonEmpty).map(w => (uri, w.toLowerCase))
    }  // (d, t)

    val uriToWordAndCount = uriToWords
      .countByValue  // ((d, t), tf)
      .map(t => (t._1._1, (t._1._2, t._2)))  // (d, (t, tf))

    val wordToDf = uriToWords.distinct.values.countByValue  // (t, df)
      .cross(uriToContent.keys.distinct.count)  // N
      .map { case ((t, df), numDocs) => (t, df.toDouble / numDocs) }  // (t, df/N)

    uriToWords.keys.countByValue  // (d, |d|)
      .join(uriToWordAndCount)  // (d, (|d|, (t, tf)))
      .map { case (d, (dl, (t, tf))) => (t, (d, tf.toDouble / dl)) }  // (t, (d, tf/|d|))
      .join(wordToDf)  // (t, ((d, tf/|d|), df/N))
      .map { case (t, ((d, tf), df)) =>
        Seq(t, d, tf * math.log(1 / df)).mkString("\t")  // (t, d, tf/|d|*log(N/df)
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
  // scalastyle:on method.length
}
