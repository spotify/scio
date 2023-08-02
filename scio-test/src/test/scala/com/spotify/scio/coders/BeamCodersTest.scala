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

package com.spotify.scio.coders

import com.spotify.scio.ScioContext
import org.apache.beam.sdk.coders.{
  BigEndianShortCoder,
  ByteCoder,
  Coder => BCoder,
  StringUtf8Coder,
  StructuredCoder,
  VarIntCoder
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.io.{InputStream, OutputStream}
import java.util.{Arrays => JArrays, List => JList}

object BeamCodersTest {
  class CustomKeyValueCoder[K, V](keyCoder: BCoder[K], valueCoder: BCoder[V])
      extends StructuredCoder[(String, Int)] {
    override def encode(value: (String, Int), outStream: OutputStream): Unit = ???
    override def decode(inStream: InputStream): (String, Int) = ???
    override def getCoderArguments: JList[_ <: BCoder[_]] =
      JArrays.asList(keyCoder, valueCoder)
    override def verifyDeterministic(): Unit = ???
  }
}

class BeamCodersTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  import BeamCodersTest._

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
      Coder.gen,
      Coder.beam(new CustomKeyValueCoder(StringUtf8Coder.of(), VarIntCoder.of()))
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
