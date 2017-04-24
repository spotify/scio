package com.spotify.scio.coders

import org.joda.time.{LocalDate, LocalDateTime}
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest._
import org.scalatest.prop.Checkers

import scala.util.Try

class JodaSerializerTest extends FlatSpec with Checkers {
  implicit val localDateTimeArb = Arbitrary {
    for {
      year <- Gen.choose(-292275054, 292278993)
      month <- Gen.choose(1, 12)
      maxDayOfMonth <- Try {
        Gen.const(new LocalDateTime(year, month, 1, 0, 0).dayOfMonth().getMaximumValue)
      } getOrElse Gen.fail
      day <- Gen.choose(1, maxDayOfMonth)
      hour <- Gen.choose(0, 23)
      minute <- Gen.choose(0, 59)
      second <- Gen.choose(0, 59)
      ms <- Gen.choose(0, 999)
      attempt <- Try {
        val ldt = new LocalDateTime(year, month, day, hour, minute, second, ms)
        Gen.const(ldt)
      } getOrElse Gen.fail
    } yield attempt
  }

  implicit val localDateArb = Arbitrary {
    Arbitrary.arbitrary[LocalDateTime].map(_.toLocalDate)
  }

  behavior of "KryoAtomicCoder"

  val coder = KryoAtomicCoder[Any]

  def roundTripProp[T](value: T): Prop = Prop.secure {
    CoderTestUtils.testRoundTrip(coder, value)
  }

  it should "roundtrip LocalDate" in {
    check(roundTripProp[LocalDate] _)
  }

  it should "roundtrip LocalDateTime" in {
    check(roundTripProp[LocalDateTime] _)
  }
}
