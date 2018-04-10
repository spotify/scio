/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.testing

import com.spotify.scio.streaming.ACCUMULATING_FIRED_PANES
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.transforms.windowing.{
  AfterProcessingTime,
  AfterWatermark,
  IntervalWindow
}
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration, Instant}

// scalastyle:off no.whitespace.before.left.bracket
class SCollectionMatchersTest extends PipelineSpec {

  "SCollectionMatchers" should "support containInAnyOrder" in {
    // should cases
    runWithContext {
      _.parallelize(1 to 100) should containInAnyOrder (1 to 100)
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 200) should containInAnyOrder (1 to 100) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should containInAnyOrder (1 to 200) }
    }

    // shouldNot cases
    runWithContext {
      _.parallelize(1 to 100) shouldNot containInAnyOrder (1 to 10)
    }
    runWithContext {
      _.parallelize(1 to 10) shouldNot containInAnyOrder (1 to 100)
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) shouldNot containInAnyOrder (1 to 100) }
    }
  }

  it should "support containSingleValue" in {
    // should cases
    runWithContext { _.parallelize(Seq(1)) should containSingleValue (1) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) should containSingleValue (10) }
    }
    an [PipelineExecutionException] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) should containSingleValue (1) }
    }
    an [PipelineExecutionException] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) should containSingleValue (1) }
    }

    // shouldNot cases
    runWithContext { _.parallelize(Seq(10)) shouldNot containSingleValue (1) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) shouldNot containSingleValue (1) }
    }
    an [PipelineExecutionException] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) shouldNot containSingleValue (1) }
    }
    an [PipelineExecutionException] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) shouldNot containSingleValue (1) }
    }
  }

  it should "support containValue" in {
    // should cases
    runWithContext { _.parallelize(Seq(1, 2, 3)) should containValue (1) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) should containValue (10) }
    }
    // shouldNot cases
    runWithContext { _.parallelize(Seq(1, 2, 3)) shouldNot containValue (4) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1, 2, 3)) shouldNot containValue (1) }
    }
  }

  it should "support beEmpty" in {
    // should cases
    runWithContext { _.parallelize(Seq.empty[Int]) should beEmpty }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) should beEmpty }
    }

    // shouldNot cases
    runWithContext { _.parallelize(1 to 10) shouldNot beEmpty }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) shouldNot beEmpty }
    }
  }

  it should "support haveSize" in {
    // should cases
    runWithContext { _.parallelize(Seq.empty[Int]) should haveSize (0) }
    runWithContext { _.parallelize(Seq(1)) should haveSize (1) }
    runWithContext { _.parallelize(1 to 10) should haveSize (10) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) should haveSize (1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) should haveSize (0) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) should haveSize (20) }
    }

    // shouldNot cases
    runWithContext { _.parallelize(Seq.empty[Int]) shouldNot haveSize (1) }
    runWithContext { _.parallelize(Seq(1)) shouldNot haveSize (0) }
    runWithContext { _.parallelize(1 to 10) shouldNot haveSize (100) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[Int]) shouldNot haveSize (0) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq(1)) shouldNot haveSize (1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 10) shouldNot haveSize (10) }
    }
  }

  it should "support equalMapOf" in {
    // should cases
    val s = Seq("a" -> 1, "b" -> 2, "c" -> 3)
    runWithContext { sc =>
      sc.parallelize(s) should equalMapOf (s.toMap)
      sc.parallelize(Seq.empty[(String, Int)]) should equalMapOf (Map.empty[String, Int])
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf ((s :+ "d" -> 4).toMap) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s :+ "d" -> 4) should equalMapOf (s.toMap) }
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf (s.tail.toMap) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s.tail) should equalMapOf (s.toMap) }
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf (s.toMap + ("a" -> 10)) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s.tail :+ ("a" -> 10)) should equalMapOf (s.toMap) }
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) should equalMapOf (Map.empty[String, Int]) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(Seq.empty[(String, Int)]) should equalMapOf (s.toMap) }
    }

    // shouldNot cases
    runWithContext { sc =>
      sc.parallelize(s) shouldNot equalMapOf ((s :+ "d" -> 4).toMap)
      sc.parallelize(s) shouldNot equalMapOf (Map.empty[String, Int])
      sc.parallelize(Seq.empty[(String, Int)]) shouldNot equalMapOf (s.toMap)
    }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(s) shouldNot equalMapOf (s.toMap) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[(String, Int)]) shouldNot equalMapOf (Map.empty[String, Int])
      }
    }
  }

  it should "support satisfy" in {
    // should cases
    runWithContext { _.parallelize(1 to 100) should satisfy[Int] (_.sum == 5050) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should satisfy[Int] (_.sum == 100) }
    }

    // shouldNot cases
    runWithContext { _.parallelize(1 to 100) shouldNot satisfy[Int] (_.sum == 100) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) shouldNot satisfy[Int] (_.sum == 5050) }
    }
  }

  it should "support forAll" in {
    // should cases
    runWithContext { _.parallelize(1 to 100) should forAll[Int] (_ > 0) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should forAll[Int] (_ > 10) }
    }

    // shouldNot cases
    runWithContext { _.parallelize(1 to 100) shouldNot forAll[Int] (_ > 10) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) shouldNot forAll[Int] (_ > 0) }
    }
  }

  it should "support tolerance" in {
    val xs = Seq(1.4, 1.5, 1.6)

    // should cases
    runWithContext { _.parallelize(xs) should forAll[Double] (_ === 1.5+-0.1) }
    runWithContext { _.parallelize(xs) should exist[Double] (_ === 1.5+-0.1) }
    runWithContext { _.parallelize(xs) should satisfy[Double] (_.sum === 5.0+-0.5) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(xs) should forAll[Double] (_ === 1.4+-0.1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(xs) should exist[Double] (_ === 1.0+-0.1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(xs) should satisfy[Double] (_.sum === 1.0+-0.5) }
    }

    // shouldNot cases
    runWithContext { _.parallelize(xs) shouldNot forAll[Double] (_ === 1.4+-0.1) }
    runWithContext { _.parallelize(xs) shouldNot exist[Double] (_ === 1.0+-0.1) }
    runWithContext { _.parallelize(xs) shouldNot satisfy[Double] (_.sum === 1.0+-0.5) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(xs) shouldNot forAll[Double] (_ === 1.5+-0.1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(xs) shouldNot exist[Double] (_ === 1.5+-0.1) }
    }
    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(xs) shouldNot satisfy[Double] (_.sum === 5.0+-0.5) }
    }
  }

  it should "support exist" in {
    // should cases
    runWithContext { _.parallelize(1 to 100) should exist[Int] (_ > 99) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) should exist[Int] (_ > 100) }
    }

    // shouldNot cases
    runWithContext { _.parallelize(1 to 100) shouldNot exist[Int] (_ > 100) }

    an [AssertionError] should be thrownBy {
      runWithContext { _.parallelize(1 to 100) shouldNot exist[Int] (_ > 99) }
    }
  }

  it should "support windowing" in {
    val allowedLateness = Duration.standardHours(1)
    val teamWindowDuration = Duration.standardMinutes(20)
    val baseTime = new Instant(0)
    def event[A](elem: A, baseTimeOffset: Duration): TimestampedValue[A] =
      TimestampedValue.of(elem, baseTime.plus(baseTimeOffset))

    val stream = testStreamOf[Int]
      // Start at the epoch
      .advanceWatermarkTo(baseTime)
      // add some elements ahead of the watermark
      .addElements(event(1, Duration.standardSeconds(3)),
      event(2, Duration.standardMinutes(1)),
      event(3, Duration.standardSeconds(22)),
      event(4, Duration.standardSeconds(3)))
      // The watermark advances slightly, but not past the end of the window
      .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
      .addElements(event(1, Duration.standardMinutes(4)),
        event(2, Duration.standardSeconds(270)))

    runWithContext { sc =>
      val windowedStream = sc
        .testStream(stream.advanceWatermarkToInfinity())
        .withFixedWindows(
          teamWindowDuration,
          options = WindowOptions(
            trigger = AfterWatermark
              .pastEndOfWindow()
              .withEarlyFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(5)))
              .withLateFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(10))),
            accumulationMode = ACCUMULATING_FIRED_PANES,
            allowedLateness = allowedLateness
          )
        )
        .withTimestamp

      val window = new IntervalWindow(baseTime, teamWindowDuration)
      windowedStream.groupByKey.keys should inOnTimePane(window) {
        containInAnyOrder(Seq(1, 2, 3, 4))
      }

      windowedStream.groupByKey should inFinalPane(window) {
        haveSize(4)
      }

      windowedStream.map(_._1).sum should inOnlyPane(window) {
        containSingleValue(13)
      }

      an [ClassCastException] should be thrownBy {
        windowedStream.groupByKey should inEarlyGlobalWindowPanes {
          haveSize(4)
        }
      }

      windowedStream.groupByKey should inWindow(window) {
        forAll[(Int, Iterable[Instant])] {
          case (_, seq) => seq.nonEmpty
        }
      }

    }

    runWithContext { sc =>
      val windowedStream = sc
        .testStream(
          stream
            .advanceProcessingTime(Duration.standardMinutes(21))
            .advanceWatermarkToInfinity())
        .withGlobalWindow(options = WindowOptions(
          trigger = AfterWatermark
            .pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(Duration.standardMinutes(5))),
          accumulationMode = ACCUMULATING_FIRED_PANES,
          allowedLateness = allowedLateness
        ))

      windowedStream.sum should inEarlyGlobalWindowPanes {
        containInAnyOrder(Iterable(13))
      }

      windowedStream.sum shouldNot inEarlyGlobalWindowPanes {
        beEmpty
      }

    }
  }

}
// scalastyle:on no.whitespace.before.left.bracket
