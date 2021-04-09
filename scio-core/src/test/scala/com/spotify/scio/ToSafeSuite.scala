package com.spotify.scio

import com.spotify.scio.schemas.To

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Source(b: Boolean)
case class Dest(b: Boolean)

class ToSafeTest extends AnyFlatSpec with Matchers {
  To.safe[Source, Dest].convert(Source(true))

  // "To.safe" should "generate an unchecked conversion on compatible case class schemas" in {
  //   To.safe[Source, Dest].convert(Source(true)) shouldBe Dest(true)
  // }
}
