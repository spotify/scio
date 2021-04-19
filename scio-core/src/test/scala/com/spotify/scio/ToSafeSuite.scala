package com.spotify.scio

import com.spotify.scio.schemas.To

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Source(b: Boolean)
case class Dest(b: Boolean)
case class Mistake(b: Int)
case class Mistake2(c: Boolean)

case class Sources(name: String, links: List[Array[Byte]])
case class Destinations(name: String, links: List[Array[Byte]])
case class DestinationsWrong(name: String, links: List[Array[Int]])

class ToSafeTest extends AnyFlatSpec with Matchers {
  "To.safe" should "generate a conversion on compatible flat case class schemas" in {
    To.safe[Source, Dest]
  }

  it should "fail on incompatible flat case class schemas" in {
    "To.safe[Source, Mistake2]" shouldNot compile
    "To.safe[Source, Mistake]" shouldNot compile
  }

  it should "generate a conversion on compatible nested case class schemas" in {
    To.safe[Sources, Destinations]
  }

  it should "fail on incompatible nested case class schemas" in {
    "To.safe[Sources, DestinationsWrong]" shouldNot compile
  }
}
