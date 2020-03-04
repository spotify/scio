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

package com.spotify.scio.testing

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.streaming.ACCUMULATING_FIRED_PANES
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.transforms.windowing.{
  AfterProcessingTime,
  AfterWatermark,
  IntervalWindow,
  Repeatedly
}
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration, Instant}
import java.io.ObjectOutputStream

import scala.util.Try
import java.io.ObjectInputStream
import java.io.IOException
import java.io.NotSerializableException
import cats.kernel.Eq

object SCollectionMatchersTest {
  // intentionally not serializable to test lambda ser/de
  class TestRecord(val x: Int) {
    override def hashCode(): Int = x
    override def equals(obj: Any): Boolean =
      obj.isInstanceOf[TestRecord] && x == obj.asInstanceOf[TestRecord].x
  }
}

final case class DoesNotSerialize(a: String, b: Int) extends Serializable {
  @throws(classOf[IOException])
  private def writeObject(o: ObjectOutputStream): Unit =
    throw new NotSerializableException("DoesNotSerialize can't be serialized")
  @throws(classOf[IOException])
  private def readObject(o: ObjectInputStream): Unit =
    throw new NotSerializableException("DoesNotSerialize can't be serialized")
}

class SCollectionMatchersTest extends PipelineSpec {
  import SCollectionMatchersTest.TestRecord
  implicit val coder = Coder.kryo[TestRecord]
  private def newTR(x: Int) = new TestRecord(x)

  "SCollectionMatchers" should "support containInAnyOrder" in {
    // should cases
    runWithContext {
      _.parallelize(1 to 100) should containInAnyOrder(1 to 100)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 200) should containInAnyOrder(1 to 100)
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 100) should containInAnyOrder(1 to 200)
      }
    }

    // shouldNot cases
    runWithContext {
      _.parallelize(1 to 100) shouldNot containInAnyOrder(1 to 10)
    }
    runWithContext {
      _.parallelize(1 to 10) shouldNot containInAnyOrder(1 to 100)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 100) shouldNot containInAnyOrder(1 to 100)
      }
    }

    // lambda ser/de
    runWithContext(_.parallelize(Seq(newTR(1))) should containInAnyOrder(Seq(newTR(1))))
    runWithContext(_.parallelize(Seq(newTR(1))) shouldNot containInAnyOrder(Seq(newTR(2))))
  }

  it should "support containSingleValue" in {
    // should cases
    runWithContext(_.parallelize(Seq(1)) should containSingleValue(1))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1)) should containSingleValue(10))
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext(_.parallelize(1 to 10) should containSingleValue(1))
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[Int]) should containSingleValue(1)
      }
    }

    // shouldNot cases
    runWithContext(_.parallelize(Seq(10)) shouldNot containSingleValue(1))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1)) shouldNot containSingleValue(1))
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext(_.parallelize(1 to 10) shouldNot containSingleValue(1))
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[Int]) shouldNot containSingleValue(1)
      }
    }

    // lambda ser/de
    runWithContext(_.parallelize(Seq(newTR(1))) should containSingleValue(newTR(1)))
    runWithContext(_.parallelize(Seq(newTR(1))) shouldNot containSingleValue(newTR(2)))
  }

  it should "support containValue" in {
    // should cases
    runWithContext(_.parallelize(Seq(1, 2, 3)) should containValue(1))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1)) should containValue(10))
    }

    // shouldNot cases
    runWithContext(_.parallelize(Seq(1, 2, 3)) shouldNot containValue(4))

    runWithContext(_.parallelize(Seq(1, 2, 3)) should not(containValue(4)))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1, 2, 3)) shouldNot containValue(1))
    }

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1, 2, 3)) should not(containValue(1)))
    }

    // lambda ser/de
    runWithContext(_.parallelize(Seq(newTR(1), newTR(2))) should containValue(newTR(1)))
    runWithContext(_.parallelize(Seq(newTR(1), newTR(2))) shouldNot containValue(newTR(3)))
  }

  it should "support beEmpty" in {
    // should cases
    runWithContext(_.parallelize(Seq.empty[Int]) should beEmpty)

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 10) should beEmpty)
    }

    // shouldNot cases
    runWithContext(_.parallelize(1 to 10) shouldNot beEmpty)

    runWithContext(_.parallelize(1 to 10) should not(beEmpty))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq.empty[Int]) shouldNot beEmpty)
    }

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq.empty[Int]) should not(beEmpty))
    }
  }

  it should "support haveSize" in {
    // should cases
    runWithContext(_.parallelize(Seq.empty[Int]) should haveSize(0))
    runWithContext(_.parallelize(Seq(1)) should haveSize(1))
    runWithContext(_.parallelize(1 to 10) should haveSize(10))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq.empty[Int]) should haveSize(1))
    }
    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1)) should haveSize(0))
    }
    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 10) should haveSize(20))
    }

    // shouldNot cases
    runWithContext(_.parallelize(Seq.empty[Int]) shouldNot haveSize(1))
    runWithContext(_.parallelize(Seq(1)) shouldNot haveSize(0))
    runWithContext(_.parallelize(1 to 10) shouldNot haveSize(100))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq.empty[Int]) shouldNot haveSize(0))
    }
    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(Seq(1)) shouldNot haveSize(1))
    }
    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 10) shouldNot haveSize(10))
    }
  }

  it should "support equalMapOf" in {
    // should cases
    val s = Seq("a" -> 1, "b" -> 2, "c" -> 3)
    runWithContext { sc =>
      sc.parallelize(s) should equalMapOf(s.toMap)
      sc.parallelize(Seq.empty[(String, Int)]) should equalMapOf(Map.empty[String, Int])
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(s) should equalMapOf((s :+ "d" -> 4).toMap)
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(s :+ "d" -> 4) should equalMapOf(s.toMap))
    }

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(s) should equalMapOf(s.tail.toMap))
    }
    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(s.tail) should equalMapOf(s.toMap))
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(s) should equalMapOf(s.toMap + ("a" -> 10))
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(s.tail :+ ("a" -> 10)) should equalMapOf(s.toMap)
      }
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(s) should equalMapOf(Map.empty[String, Int])
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[(String, Int)]) should equalMapOf(s.toMap)
      }
    }

    // shouldNot cases
    runWithContext { sc =>
      sc.parallelize(s) shouldNot equalMapOf((s :+ "d" -> 4).toMap)
      sc.parallelize(s) shouldNot equalMapOf(Map.empty[String, Int])
      sc.parallelize(Seq.empty[(String, Int)]) shouldNot equalMapOf(s.toMap)
    }

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(s) shouldNot equalMapOf(s.toMap))
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[(String, Int)]) shouldNot equalMapOf(Map.empty[String, Int])
      }
    }

    // lambda ser/de
    val s2 = Seq("a" -> newTR(1), "b" -> newTR(2))
    runWithContext(_.parallelize(s2) should equalMapOf(s2.toMap))
    runWithContext(_.parallelize(s2) shouldNot equalMapOf((s2 :+ "c" -> newTR(3)).toMap))
  }

  it should "support satisfy" in {
    // should cases
    runWithContext {
      _.parallelize(1 to 100) should satisfy[Int](_.sum == 5050)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 100) should satisfy[Int](_.sum == 100)
      }
    }

    // shouldNot cases
    runWithContext {
      _.parallelize(1 to 100) shouldNot satisfy[Int](_.sum == 100)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 100) shouldNot satisfy[Int](_.sum == 5050)
      }
    }

    // lambda ser/de
    // FIXME: these will fail if TR in predicate is pulled in via closure, not sure if fixable
    runWithContext {
      _.parallelize(Seq(newTR(1))) should satisfy[TestRecord](_.toList.contains(newTR(1)))
    }
    runWithContext {
      _.parallelize(Seq(newTR(1))) shouldNot satisfy[TestRecord](_.toList.contains(newTR(2)))
    }
  }

  it should "support satisfySingleValue" in {
    // should cases
    runWithContext {
      _.parallelize(Seq(1)) should satisfySingleValue[Int](_ == 1)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(Seq(1)) should satisfySingleValue[Int](_ == 10)
      }
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 10) should satisfySingleValue[Int](_ == 1)
      }
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[Int]) should satisfySingleValue[Int](_ == 1)
      }
    }

    // shouldNot cases
    runWithContext {
      _.parallelize(Seq(10)) shouldNot satisfySingleValue[Int](_ == 1)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(Seq(1)) shouldNot satisfySingleValue[Int](_ == 1)
      }
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext {
        _.parallelize(1 to 10) shouldNot satisfySingleValue[Int](_ == 1)
      }
    }
    a[PipelineExecutionException] should be thrownBy {
      runWithContext {
        _.parallelize(Seq.empty[Int]) shouldNot satisfySingleValue[Int](_ == 1)
      }
    }

    // lambda ser/de
    // FIXME: these will fail if TR in predicate is pulled in via closure, not sure if fixable
    runWithContext {
      _.parallelize(Seq(newTR(1))) should satisfySingleValue[TestRecord](_ == newTR(1))
    }
    runWithContext {
      _.parallelize(Seq(newTR(1))) shouldNot satisfySingleValue[TestRecord](_ == newTR(2))
    }
  }

  it should "support satisfy when the closure does not serialize" in {
    runWithContext { ctx =>
      import CoderAssertions._
      import org.apache.beam.sdk.util.SerializableUtils

      val v = new DoesNotSerialize("foo", 42)
      val coder = CoderMaterializer.beam(ctx, Coder[DoesNotSerialize])

      assume(Try(SerializableUtils.ensureSerializable(v)).isFailure)
      assume(Try(SerializableUtils.ensureSerializableByCoder(coder, v, "?")).isSuccess)

      v coderShould roundtrip()
      coderIsSerializable[DoesNotSerialize]

      val coll = ctx.parallelize(List(v))
      coll shouldNot beEmpty // just make sure the SCollection can be built
      coll should satisfySingleValue[DoesNotSerialize](_.a == v.a)
    }
  }

  it should "support forAll" in {
    // should cases
    runWithContext(_.parallelize(1 to 100) should forAll[Int](_ > 0))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 100) should forAll[Int](_ > 10))
    }

    // shouldNot cases
    runWithContext(_.parallelize(1 to 100) shouldNot forAll[Int](_ > 10))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 100) shouldNot forAll[Int](_ > 0))
    }

    // lambda ser/de
    // FIXME: these will fail if TR in predicate is pulled in via closure, not sure if fixable
    runWithContext(_.parallelize(Seq(newTR(1))) should forAll[TestRecord](_ == newTR(1)))
    runWithContext(_.parallelize(Seq(newTR(1))) shouldNot forAll[TestRecord](_ == newTR(2)))
  }

  it should "support tolerance" in {
    val xs = Seq(1.4, 1.5, 1.6)

    // should cases
    runWithContext(_.parallelize(xs) should forAll[Double](_ === 1.5 +- 0.1))
    runWithContext(_.parallelize(xs) should exist[Double](_ === 1.5 +- 0.1))
    runWithContext {
      _.parallelize(xs) should satisfy[Double](_.sum === 5.0 +- 0.5)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(xs) should forAll[Double](_ === 1.4 +- 0.1)
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(xs) should exist[Double](_ === 1.0 +- 0.1)
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(xs) should satisfy[Double](_.sum === 1.0 +- 0.5)
      }
    }

    // shouldNot cases
    runWithContext {
      _.parallelize(xs) shouldNot forAll[Double](_ === 1.4 +- 0.1)
    }
    runWithContext {
      _.parallelize(xs) shouldNot exist[Double](_ === 1.0 +- 0.1)
    }
    runWithContext {
      _.parallelize(xs) shouldNot satisfy[Double](_.sum === 1.0 +- 0.5)
    }

    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(xs) shouldNot forAll[Double](_ === 1.5 +- 0.1)
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(xs) shouldNot exist[Double](_ === 1.5 +- 0.1)
      }
    }
    an[AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(xs) shouldNot satisfy[Double](_.sum === 5.0 +- 0.5)
      }
    }
  }

  it should "support exist" in {
    // should cases
    runWithContext(_.parallelize(1 to 100) should exist[Int](_ > 99))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 100) should exist[Int](_ > 100))
    }

    // shouldNot cases
    runWithContext(_.parallelize(1 to 100) shouldNot exist[Int](_ > 100))

    an[AssertionError] should be thrownBy {
      runWithContext(_.parallelize(1 to 100) shouldNot exist[Int](_ > 99))
    }

    // lambda ser/de
    // FIXME: these will fail if TR in predicate is pulled in via closure, not sure if fixable
    runWithContext(_.parallelize(Seq(newTR(1))) should exist[TestRecord](_ == newTR(1)))
    runWithContext(_.parallelize(Seq(newTR(1))) shouldNot exist[TestRecord](_ == newTR(2)))
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
      .addElements(
        event(1, Duration.standardSeconds(3)),
        event(2, Duration.standardMinutes(1)),
        event(3, Duration.standardSeconds(22)),
        event(4, Duration.standardSeconds(3))
      )
      // The watermark advances slightly, but not past the end of the window
      .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
      .addElements(event(1, Duration.standardMinutes(4)), event(2, Duration.standardSeconds(270)))

    runWithContext { sc =>
      val windowedStream = sc
        .testStream(stream.advanceWatermarkToInfinity())
        .withFixedWindows(
          teamWindowDuration,
          options = WindowOptions(
            trigger = Repeatedly
              .forever(
                AfterWatermark
                  .pastEndOfWindow()
                  .withEarlyFirings(
                    AfterProcessingTime
                      .pastFirstElementInPane()
                      .plusDelayOf(Duration.standardMinutes(5))
                  )
                  .withLateFirings(
                    AfterProcessingTime
                      .pastFirstElementInPane()
                      .plusDelayOf(Duration.standardMinutes(10))
                  )
              ),
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

      a[ClassCastException] should be thrownBy {
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
            .advanceWatermarkToInfinity()
        )
        .withGlobalWindow(
          options = WindowOptions(
            trigger = Repeatedly.forever(
              AfterWatermark
                .pastEndOfWindow()
                .withEarlyFirings(
                  AfterProcessingTime
                    .pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(5))
                )
            ),
            accumulationMode = ACCUMULATING_FIRED_PANES,
            allowedLateness = allowedLateness
          )
        )

      windowedStream.sum should inEarlyGlobalWindowPanes {
        containInAnyOrder(Iterable(13))
      }

      windowedStream.sum shouldNot inEarlyGlobalWindowPanes {
        beEmpty
      }
    }
  }

  it should "customize equality" in {
    val in = 1 to 10
    val out = 2 to 11

    runWithContext {
      _.parallelize(in) shouldNot containInAnyOrder(out)
    }

    runWithContext {
      implicit val eqW = Eqv.always
      _.parallelize(in) should containInAnyOrder(out)
    }

    runWithContext {
      _.parallelize(List(1)) shouldNot containSingleValue(2)
    }

    runWithContext {
      implicit val eqW = Eqv.always
      _.parallelize(List(1)) should containSingleValue(2)
    }

    runWithContext {
      _.parallelize(List(1, 3, 4, 5)) shouldNot containValue(2)
    }

    runWithContext {
      implicit val eqW = Eqv.always
      _.parallelize(List(1, 3, 4, 5)) should containValue(2)
    }
  }
}

object Eqv {
  def always: Eq[Int] =
    new Eq[Int] {
      def eqv(x: Int, y: Int): Boolean =
        true
    }
}
