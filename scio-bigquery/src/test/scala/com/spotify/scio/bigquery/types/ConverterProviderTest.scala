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

package com.spotify.scio.bigquery.types

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

class ConverterProviderTest extends FlatSpec with Matchers {

  it should "successfully create a None for Option field in BQ" in {
    import Test._
    val sOpt = SimpleOption(None)
    val row = SimpleOption.toTableRow(sOpt)
    val roundtrip = SimpleOption.fromTableRow(row)
    sOpt shouldBe roundtrip
  }
  it should "successfully create a Some for Option field in BQ" in {
    import Test._
    val sOpt = SimpleOption(Some("test"))
    val row = SimpleOption.toTableRow(sOpt)
    val roundtrip = SimpleOption.fromTableRow(row)
    sOpt shouldBe roundtrip
  }
//  it should "successfully create a None for Option field in BQ if null passed in" in {
//    // This throws an NPE:
////
////    [info]   java.lang.NullPointerException:
////    [info]   at com.spotify.scio.bigquery.types.Test$SimpleOption$$anonfun$toTableRow$1.apply(ConverterProviderTest.scala:84)
////      [info]   at com.spotify.scio.bigquery.types.Test$SimpleOption$$anonfun$toTableRow$1.apply(ConverterProviderTest.scala:84)
////      [info]   at com.spotify.scio.bigquery.types.ConverterProviderTest$$anonfun$3.apply(ConverterProviderTest.scala:46)
////      [info]   at com.spotify.scio.bigquery.types.ConverterProviderTest$$anonfun$3.apply(ConverterProviderTest.scala:42)
////      [info]   at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
////      [info]   at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
////      [info]   at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
////      [info]   at org.scalatest.Transformer.apply(Transformer.scala:22)
////      [info]   at org.scalatest.Transformer.apply(Transformer.scala:20)
////      [info]   at org.scalatest.FlatSpecLike$$anon$1.apply(FlatSpecLike.scala:1682)
////      [info]   ...
//
//    // My expectation would be that this would successfully create a table row of {} and then
//    // turn it back into a null on roundtrip.
//    import Test._
//    val sOpt = SimpleOption(null)
//    val row = SimpleOption.toTableRow(sOpt)
//    val roundtrip = SimpleOption.fromTableRow(row)
//    sOpt shouldBe roundtrip
//  }
  it should "successfully create a List for repeated field in BQ" in {
    import Test._
    val s = SimpleRepeated(List())
    val row = SimpleRepeated.toTableRow(s)
    val roundtrip = SimpleRepeated.fromTableRow(row)
    s shouldBe roundtrip
  }
//  it should "throw a meaningful NPE for null repeated field in BQ" in {
//    // This throws an NPE:
////    [info]   java.lang.NullPointerException:
////    [info]   at com.spotify.scio.bigquery.types.Test$SimpleRepeated$$anonfun$toTableRow$2.apply(ConverterProviderTest.scala:103)
////      [info]   at com.spotify.scio.bigquery.types.Test$SimpleRepeated$$anonfun$toTableRow$2.apply(ConverterProviderTest.scala:103)
////      [info]   at com.spotify.scio.bigquery.types.ConverterProviderTest$$anonfun$4.apply(ConverterProviderTest.scala:76)
////      [info]   at com.spotify.scio.bigquery.types.ConverterProviderTest$$anonfun$4.apply(ConverterProviderTest.scala:73)
////      [info]   at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
////      [info]   at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
////      [info]   at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
////      [info]   at org.scalatest.Transformer.apply(Transformer.scala:22)
////      [info]   at org.scalatest.Transformer.apply(Transformer.scala:20)
////      [info]   at org.scalatest.FlatSpecLike$$anon$1.apply(FlatSpecLike.scala:1682)
////      [info]   ...
//
//    // My expectation would be that this would succesfully create a table row of {} and then
//    // throw the NPE on roundtrip.
//    import Test._
//    val s = SimpleRepeated(null)
//    val row = SimpleRepeated.toTableRow(s) // this line throws an NPE when test is run
//    val roundtrip = Try(SimpleRepeated.fromTableRow(row))
//    roundtrip.isFailure shouldBe true
//    roundtrip.failed.get.getMessage shouldBe "null field in REQUIRED field a"
//  }
//
//  it should "throw a meaningful NPE for null repeated field contents in BQ" in {
//    // This test fails with:
//    //[info]   null was not equal to "null field in REQUIRED field a" (ConverterProviderTest.scala:102)
//    // I thought that this test might be what was meant to succeed, since the one above it failed.
//    import Test._
//      val s = SimpleRepeated(List(null))
//      val row = SimpleRepeated.toTableRow(s) // this line throws an NPE when test is run
//      val roundtrip = Try(SimpleRepeated.fromTableRow(row))
//      roundtrip.isFailure shouldBe true
//      roundtrip.failed.get.getMessage shouldBe "null field in REQUIRED field a"
//    }
  it should "successfully create a String for typed field in BQ" in {
    import Test._
    val s = Simple("test")
    val row = Simple.toTableRow(s)
    val roundtrip = Simple.fromTableRow(row)
    s shouldBe roundtrip
  }
  it should "throw a meaningful NPE for null typed field in BQ" in {
    import Test._
    val s = Simple(null)
    val row = Simple.toTableRow(s)
    val roundtrip = Try(Simple.fromTableRow(row))
    roundtrip.isFailure shouldBe true
    roundtrip.failed.get.getMessage shouldBe "null field in REQUIRED field a"
  }

}

object Test {
  @BigQueryType.toTable
  case class SimpleOption(a: Option[String])

  @BigQueryType.toTable
  case class SimpleRepeated(a: List[String])

  @BigQueryType.toTable
  case class Simple(a: String)
}