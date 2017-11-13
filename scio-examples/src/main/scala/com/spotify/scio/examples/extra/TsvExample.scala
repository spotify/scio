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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.{io => gio}

/*
SBT
runMain
  com.spotify.scio.examples.extra.TsvExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://apache-beam-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

/**
 * Word count example writing to TSV file.
 * To read TSV/CSV see the TrafficRoutes.scala example.
 * Currently there is no simple way on Beam SDK to read TSV/CSV skiping header row.
 * See:
 * - https://issues.apache.org/jira/browse/BEAM-51
 * - https://issues.apache.org/jira/browse/BEAM-123
 */
object TsvExample {

  private def pathWithShards(path: String) =
    path.replaceAll("\\/+$", "") + "/part"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val output: String = args("output")

    val transform = gio.TextIO.write()
      .to(pathWithShards(output))
      .withSuffix(".tsv")
      .withNumShards(1)
      .withHeader("word\tcount")

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => Seq(t._1, t._2.toString))
      .map(_.mkString("\t"))
      .saveAsCustomOutput(output, transform)
    sc.close()
  }

}
