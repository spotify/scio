/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SerializedExamplesOfStandardDataSpec extends AnyWordSpec with Matchers {
  import SerializedExamplesData._

  s"""
    |Projects using chill to persist serialized data (for example in event sourcing
    |scenarios) depend on the serialized representation of the pre-registered
    |classes being stable. Therefore, it is important that updates to chill avoid
    |changing the serialization of pre-registered classes as far as possible.
    |When changing a serialization becomes necessary, details of the changes should
    |be mentioned in the release notes.
    |
    |This spec verifies for all registered classes of ScalaKryoInstantiators
    |- that the serialization class id is as expected,
    |- that a scala instance serializes to the expected binary representation,
    |- that the binary representation after deserializing & serializing
    |  does not change.
    |Note that it would be difficult to implement an equals check comparing
    |the scala instance with the instance obtained from deserialization, so
    |this is not implemented here.
    |
    |With ScalaKryoInstantiators, the registered classes""".stripMargin
    .should {
      "serialize as expected to the correct value (see above for details)"
        .in {
          val scalaVersion = scala.util.Properties.versionNumberString
          val examplesToOmit = OmitExamplesInScalaVersion
            .filterKeys(scalaVersion.startsWith)
            .values
            .flatten
            .toSet
          Examples.foreach {
            case (serId, (serialized, (scala: AnyRef, useObjectEquality: Boolean))) =>
              if (examplesToOmit.contains(serId))
                println(
                  s"### SerializedExamplesOfStandardDataSpec: Omitting $serId in scala $scalaVersion"
                )
              else
                checkSerialization(serialized, serId, scala, useObjectEquality)
            case (serId, (serialized, scala)) =>
              if (examplesToOmit.contains(serId))
                println(
                  s"### SerializedExamplesOfStandardDataSpec: Omitting $serId in scala $scalaVersion"
                )
              else
                checkSerialization(serialized, serId, scala, useObjectEquality = false)
          }
        }
      "all be covered by an example".in {
        val serIds = Examples.map(_._1)
        assert(serIds == serIds.distinct, "duplicate keys in examples map detected")
        val exampleStrings = Examples.map(_._2._1)
        assert(
          exampleStrings == exampleStrings.distinct,
          "duplicate example strings in examples map detected"
        )
        assert(
          (serIds ++ SpecialCasesNotInExamplesMap).sorted ==
            Seq.range(0, kryo.getNextRegistrationId),
          s"there are approx ${kryo.getNextRegistrationId - serIds.size - SpecialCasesNotInExamplesMap.size} " +
            "examples missing for preregistered classes"
        )
      }
    }

  val kryo: KryoBase = {
    val instantiator = new ScalaKryoInstantiator()
    instantiator.setRegistrationRequired(true)
    instantiator.newKryo
  }
  val pool: KryoPool =
    KryoPool.withByteArrayOutputStream(4, new ScalaKryoInstantiator())
  def err(message: String): Unit =
    System.err.println(s"\n##########\n$message\n##########\n")
  def err(message: String, serialized: String): Unit =
    System.err.println(
      s"\n##########\n$message\nThe example serialized is $serialized\n##########\n"
    )

  def checkSerialization(
    serializedExample: String,
    expectedSerializationId: Int,
    scalaInstance: AnyRef,
    useObjectEquality: Boolean
  ): Unit = {
    val idForScalaInstance = kryo.getRegistration(scalaInstance.getClass).getId
    assert(
      idForScalaInstance == expectedSerializationId,
      s"$scalaInstance is registered with ID $idForScalaInstance, but expected $expectedSerializationId"
    )

    val serializedScalaInstance =
      try Base64.encodeBytes(pool.toBytesWithClass(scalaInstance))
      catch {
        case e: Throwable =>
          err(s"can't kryo serialize $scalaInstance: $e")
          throw e
      }
    assert(
      serializedScalaInstance == serializedExample,
      s"$scalaInstance with serialization id $idForScalaInstance serializes to $serializedScalaInstance, " +
        s"but the test example is $serializedExample"
    )

    val bytes =
      try Base64.decode(serializedExample)
      catch {
        case e: Throwable =>
          err(
            s"can't base64 decode $serializedExample with serialization id $idForScalaInstance: $e",
            serializedScalaInstance
          )
          throw e
      }
    val deserialized =
      try pool.fromBytes(bytes)
      catch {
        case e: Throwable =>
          err(
            s"can't kryo deserialize $serializedExample with serialization id $idForScalaInstance: $e",
            serializedScalaInstance
          )
          throw e
      }

    // Some objects like arrays can't be deserialized and compared with "==", so the roundtrip serialization is used
    // for checking. Other objects like Queue.empty can't be roundtrip compared, so object equality is checked.
    if (useObjectEquality) {
      assert(
        deserialized == scalaInstance,
        s"deserializing $serializedExample yields $deserialized (serialization id $idForScalaInstance), " +
          s"but expected $scalaInstance which does not equal to the deserialized example and which in turn " +
          s"serializes to $serializedScalaInstance"
      )
    } else {
      val roundtrip =
        try Base64.encodeBytes(pool.toBytesWithClass(deserialized))
        catch {
          case e: Throwable =>
            err(
              s"can't kryo serialize roundtrip $deserialized with serialization id $idForScalaInstance: $e"
            )
            throw e
        }

      assert(
        roundtrip == serializedExample,
        s"deserializing $serializedExample yields $deserialized (serialization id $idForScalaInstance), " +
          s"but expected $scalaInstance which serializes to $serializedScalaInstance"
      )
    }
  }
}
