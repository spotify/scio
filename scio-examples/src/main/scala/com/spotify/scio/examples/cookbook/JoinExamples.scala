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

// Example: Different Types of Joins
// Usage:

// `sbt "runMain com.spotify.scio.examples.cookbook.JoinExamples
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --output=gs://[BUCKET]/[PATH]/join_examples"`
package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.HotKeyMethod

// ## Utilities used in all examples
object JoinUtil {
  // Function to extract event information from BigQuery `TableRow`s
  def extractEventInfo(row: TableRow): Seq[(String, String)] = {
    val countryCode = row.getString("ActionGeo_CountryCode")
    val sqlDate = row.getString("SQLDATE")
    val actor1Name = row.getString("Actor1Name")
    val sourceUrl = row.getString("SOURCEURL")
    val eventInfo = s"Date: $sqlDate, Actor1: $actor1Name, url: $sourceUrl"

    if (countryCode == null || eventInfo == null) Nil
    else Seq((countryCode, eventInfo))
  }

  // Function to extract country information from BigQuery `TableRow`s
  def extractCountryInfo(row: TableRow): (String, String) = {
    val countryCode = row.getString("FIPSCC")
    val countryName = row.getString("HumanName")
    (countryCode, countryName)
  }

  // Function to format output string
  def formatOutput(countryCode: String, countryName: String, eventInfo: String): String =
    s"Country code: $countryCode, Country name: $countryName, Event info: $eventInfo"
}

// ## Regular shuffle-based join
object JoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s
    val eventsInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.EVENT_TABLE)).flatMap(extractEventInfo)
    val countryInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.COUNTRY_TABLE)).map(extractCountryInfo)

    eventsInfo
      // Left outer join to produce `SCollection[(String, (String, Option[String]))]
      .leftOuterJoin(countryInfo)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}

// ## Join with map side input
object SideInputJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s, and then convert right hand side as
    // a `SideInput` of `Map[String, String]`
    val eventsInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.EVENT_TABLE)).flatMap(extractEventInfo)
    val countryInfo = sc
      .bigQueryTable(Table.Spec(ExampleData.COUNTRY_TABLE))
      .map(extractCountryInfo)
      .asMapSideInput

    eventsInfo
      // Replicate right hand side to all workers as a side input
      .withSideInputs(countryInfo)
      // Specialized version of `map` with access to side inputs via `SideInputContext`
      .map { (kv, side) =>
        val (countryCode, eventInfo) = kv
        // Retrieve side input value (`Map[String, String]`)
        val m = side(countryInfo)
        val countryName = m.getOrElse(countryCode, "none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      // End of side input operation, convert back to regular `SCollection`
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}

// ## Hash join
object HashJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s
    val eventsInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.EVENT_TABLE)).flatMap(extractEventInfo)
    val countryInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.COUNTRY_TABLE)).map(extractCountryInfo)

    eventsInfo
      // Hash join uses side input under the hood and is a drop-in replacement for regular join
      .hashLeftOuterJoin(countryInfo)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}

// ## Skewed join
object SkewedJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s
    val eventsInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.EVENT_TABLE)).flatMap(extractEventInfo)
    val countryInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.COUNTRY_TABLE))
        .map(extractCountryInfo)

    eventsInfo

      /**
       * Skewed join is useful when LHS contains a subset of keys with high frequency, but RHS is
       * too large to fit into memory. It uses Count Min Sketch (CMS) to estimate those frequencies.
       * Internally it identifies two groups of keys: "Hot" and the rest. Hot keys are joined using
       * Hash Join and the rest with the regular join. There are 3 ways to identify the set of hot
       * keys:
       *   - "threshold" as a cutoff frequency.
       *   - "top percentage" to specify the maximum relative part of all keys can be considered
       *     hot.
       *   - "top N" to specify the absolute number of hot keys.
       *
       * Also, there are several optional parameters in different overloads that could tune the
       * default behavior:
       *   - `sampleFraction` - the fraction to sample keys in LHS, can improve performance if less
       *     than 1.0, it may be also required to fit keys sample in memory for CMS. If you sample
       *     only 0.1 of the dataset then you need to decrease "threshold" 10 times respectively,
       *     because the latter relies on absolute frequencies detected in the sample.
       *   - `withReplacement` - if "true" it will use Poisson distribution, otherwise Bernoulli.
       *     The former will allow repeats of the same item in your sample.
       *   - `hotKeyFanout` - tune Apache Beam fanout feature when aggregating sample keys to CMS
       *     vectors. It should be a positive number and specifies intermediate nodes to
       *     redistribute aggregation over heavy-hitter keys. Tune it when "Compute CMS of LHS keys"
       *     transform has a problem of idle workers.
       *   - Params of confidence in error estimates must lie in `(0, 1)`:
       *     - `cmsEps` - One-sided error bound on the error of each point query, i.e. frequency
       *       estimate. Must lie in `(0, 1)`. Lower eps increases the accuracy by increasing a
       *       vector size for each hash function.
       *     - `cmsDelta` - A bound on the probability that a query estimate does not lie within
       *       some small interval (an interval that depends on `cmsEps`) around the truth. Lower
       *       delta increases the accuracy of CMS by increasing number of hash functions used.
       *   - `cmsSeed` - random value generator seed for CMS. No need to specify unless you
       *     deliberately expect some (non)deterministic results.
       */
      .skewedLeftOuterJoin(countryInfo, hotKeyMethod = HotKeyMethod.Threshold(100))
      .map { case (countryCode, (eventInfo, countryNameOpt)) =>
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}

// ## Sparse join
object SparseJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Import macro-generated encoders that implement Funnel to back guava Bloom filters
    import magnolify.guava.auto._

    // Extract both sides as `SCollection[(String, String)]`s
    val eventsInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.EVENT_TABLE)).flatMap(extractEventInfo)
    val countryInfo =
      sc.bigQueryTable(Table.Spec(ExampleData.COUNTRY_TABLE)).map(extractCountryInfo)

    eventsInfo
      // Sparse Join is useful when LHS is much larger than the RHS which cannot fit in memory, but
      // contains a mostly overlapping set of keys as LHS, i.e. when the intersection of keys is
      // sparse in the LHS. Requires specifying the estimation of RHS keys number to find the size
      // and number of BloomFilters that Scio would use to split the LHS into overlap and
      // intersection in a "map" step before an exact join. Having a value close to the actual
      // number improves the false positives in intermediate steps which means less shuffle.
      .sparseLeftOuterJoin(countryInfo, rhsNumKeys = 275)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}
