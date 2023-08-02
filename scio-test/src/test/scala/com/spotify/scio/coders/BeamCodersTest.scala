package com.spotify.scio.coders

import com.spotify.scio.ScioContext
import org.apache.beam.sdk.coders.{BigEndianShortCoder, ByteCoder, StringUtf8Coder, VarIntCoder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class BeamCodersTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  "BeamCoders" should "get scio coder from SCollection" in {
    val sc = ScioContext()
    val coll = sc.empty[String]()
    val coder = BeamCoders.getCoder(coll)
    coder shouldBe a[Beam[_]]
    val beamKeyCoder = coder.asInstanceOf[Beam[_]]
    beamKeyCoder.beam shouldBe StringUtf8Coder.of()
  }

  it should "get scio coders from tupled2 SCollection" in {
    val coders = Table[Coder[(String, Int)]](
      "coder",
      Coder.tuple2Coder,
      Coder.gen
    )

    forAll(coders) { coder =>
      val sc = ScioContext()
      val coll = sc.empty()(coder)
      val (keyCoder, valueCoder) = BeamCoders.getTupleCoders(coll)
      keyCoder shouldBe a[Beam[_]]
      val beamKeyCoder = keyCoder.asInstanceOf[Beam[_]]
      beamKeyCoder.beam shouldBe StringUtf8Coder.of()
      valueCoder shouldBe a[Beam[_]]
      val beamValueCoder = valueCoder.asInstanceOf[Beam[_]]
      beamValueCoder.beam shouldBe VarIntCoder.of()
    }
  }

  it should "get scio coders from tupled3 SCollection" in {
    val coders = Table[Coder[(String, Int, Short)]](
      "coder",
      Coder.tuple3Coder,
      Coder.gen
    )

    forAll(coders) { coder =>
      val sc = ScioContext()
      val coll = sc.empty()(coder)
      val (c1, c2, c3) = BeamCoders.getTuple3Coders(coll)
      c1 shouldBe a[Beam[_]]
      val beamCoder1 = c1.asInstanceOf[Beam[_]]
      beamCoder1.beam shouldBe StringUtf8Coder.of()
      c2 shouldBe a[Beam[_]]
      val beamCoder2 = c2.asInstanceOf[Beam[_]]
      beamCoder2.beam shouldBe VarIntCoder.of()
      c3 shouldBe a[Beam[_]]
      val beamCoder3 = c3.asInstanceOf[Beam[_]]
      beamCoder3.beam shouldBe BigEndianShortCoder.of()
    }
  }

  it should "get scio coders from tupled4 SCollection" in {
    val coders = Table[Coder[(String, Int, Short, Byte)]](
      "coder",
      Coder.tuple4Coder,
      Coder.gen
    )

    forAll(coders) { coder =>
      val sc = ScioContext()
      val coll = sc.empty()(coder)
      val (c1, c2, c3, c4) = BeamCoders.getTuple4Coders(coll)
      c1 shouldBe a[Beam[_]]
      val beamCoder1 = c1.asInstanceOf[Beam[_]]
      beamCoder1.beam shouldBe StringUtf8Coder.of()
      c2 shouldBe a[Beam[_]]
      val beamCoder2 = c2.asInstanceOf[Beam[_]]
      beamCoder2.beam shouldBe VarIntCoder.of()
      c3 shouldBe a[Beam[_]]
      val beamCoder3 = c3.asInstanceOf[Beam[_]]
      beamCoder3.beam shouldBe BigEndianShortCoder.of()
      c4 shouldBe a[Beam[_]]
      val beamCoder4 = c4.asInstanceOf[Beam[_]]
      beamCoder4.beam shouldBe ByteCoder.of()
    }
  }
}
