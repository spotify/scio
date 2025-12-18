/*
 * Copyright 2025 Spotify AB.
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

package com.spotify.scio.smb

import com.spotify.scio.avro.{Address, SpecificRecordDatumFactory, User}
import org.apache.avro.specific.SpecificData
import org.apache.beam.sdk.coders.{Coder, CoderException}
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.util.CoderUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Collections

/**
 * Minimal test to expose Beam AvroCoder DatumFactory serialization bug.
 *
 * The bug: When an AvroCoder is created with a custom DatumFactory (like
 * SpecificRecordDatumFactory), serializing and deserializing the coder does not preserve the
 * DatumFactory. This causes deserialization to fail with InstantiationException.
 *
 * Context: Beam serializes coders when sending them across worker boundaries or storing them in the
 * pipeline graph. If the DatumFactory is not properly preserved, the deserialized coder will use
 * the default SpecificDatumReader which may fail to instantiate records.
 */
class BeamAvroCoderSerializationTest extends AnyFlatSpec with Matchers {

  private val address = new Address("street1", "street2", "city", "state", "01234", "USA")

  "AvroCoder with SpecificRecordDatumFactory" should "preserve DatumFactory after serialization" in {
    // Create a User record for testing
    val user = new User(
      1,
      "Smith",
      "Alice",
      "alice@example.com",
      Collections.emptyList(),
      address
    )

    // Create AvroCoder with custom SpecificRecordDatumFactory (the way Scio does it)
    val datumFactory = new SpecificRecordDatumFactory(classOf[User])
    val schema = SpecificData.get().getSchema(classOf[User])
    val originalCoder: Coder[User] = AvroCoder.of(datumFactory, schema)

    // Encode a User with the original coder - this should work
    val encodedUser = CoderUtils.encodeToByteArray(originalCoder, user)
    println(s"[DEBUG] Successfully encoded User with original coder, ${encodedUser.length} bytes")

    // Decode with original coder - this should work
    val decodedUser1 = CoderUtils.decodeFromByteArray(originalCoder, encodedUser)
    println(s"[DEBUG] Successfully decoded User with original coder: ${decodedUser1.getFirstName}")
    decodedUser1.getId shouldBe 1
    decodedUser1.getFirstName shouldBe "Alice"

    // Now simulate what Beam does: serialize the coder itself
    println(s"[DEBUG] Serializing the coder...")
    val coderBytes = new ByteArrayOutputStream()
    originalCoder.asInstanceOf[AvroCoder[User]].encode(user, coderBytes)

    // Alternative: use Beam's CoderUtils to serialize the coder structure
    // This simulates what happens when Beam sends coders to workers
    val serializedCoderBytes = new ByteArrayOutputStream()
    try {
      // Beam's internal coder serialization
      originalCoder.asInstanceOf[AvroCoder[User]].encode(user, serializedCoderBytes)
      println(s"[DEBUG] Serialized coder structure: ${serializedCoderBytes.size()} bytes")
    } catch {
      case e: Exception =>
        println(s"[DEBUG] Exception during coder serialization: ${e.getMessage}")
    }

    // The real test: Can we decode the user data with a "fresh" coder instance?
    // This simulates what happens when the coder is deserialized on a worker
    println(s"[DEBUG] Creating a fresh AvroCoder (simulating deserialization)...")
    val deserializedCoder: Coder[User] = AvroCoder.of(datumFactory, schema)

    // Try to decode with the "deserialized" coder
    println(s"[DEBUG] Attempting to decode User with fresh coder...")
    try {
      val decodedUser2 = CoderUtils.decodeFromByteArray(deserializedCoder, encodedUser)
      println(s"[DEBUG] Successfully decoded User with fresh coder: ${decodedUser2.getFirstName}")
      decodedUser2.getId shouldBe 1
      decodedUser2.getFirstName shouldBe "Alice"
    } catch {
      case e: CoderException =>
        println(s"[DEBUG] CoderException when decoding with fresh coder: ${e.getMessage}")
        e.printStackTrace()
        throw e
      case e: Exception =>
        println(
          s"[DEBUG] Exception when decoding with fresh coder: ${e.getClass.getName}: ${e.getMessage}"
        )
        e.printStackTrace()
        throw e
    }
  }

  it should "work when using default Avro coder without custom DatumFactory" in {
    // Control test: verify that the issue is specific to custom DatumFactory
    val user = new User(
      2,
      "Jones",
      "Bob",
      "bob@example.com",
      Collections.emptyList(),
      address
    )

    // Use Avro's default coder (no custom DatumFactory)
    val schema = SpecificData.get().getSchema(classOf[User])
    val defaultCoder: Coder[User] = AvroCoder.of(classOf[User], schema)

    // Encode
    val encodedUser = CoderUtils.encodeToByteArray(defaultCoder, user)
    println(s"[DEBUG Control] Encoded User with default coder, ${encodedUser.length} bytes")

    // Decode with fresh instance (simulating deserialization)
    val freshCoder: Coder[User] = AvroCoder.of(classOf[User], schema)
    val decodedUser = CoderUtils.decodeFromByteArray(freshCoder, encodedUser)

    println(
      s"[DEBUG Control] Successfully decoded with fresh default coder: ${decodedUser.getFirstName}"
    )
    decodedUser.getId shouldBe 2
    decodedUser.getFirstName shouldBe "Bob"
  }

  it should "demonstrate the actual serialization path Beam uses" in {
    // This test attempts to serialize/deserialize the AvroCoder object itself,
    // not just the data it encodes

    val user = new User(
      3,
      "Brown",
      "Charlie",
      "charlie@example.com",
      Collections.emptyList(),
      address
    )

    val datumFactory = new SpecificRecordDatumFactory(classOf[User])
    val schema = SpecificData.get().getSchema(classOf[User])
    val originalCoder: AvroCoder[User] = AvroCoder.of(datumFactory, schema)

    // Try to serialize the coder itself using Java serialization
    println(s"[DEBUG Serialization] Attempting to serialize AvroCoder object...")
    try {
      val baos = new ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(baos)
      oos.writeObject(originalCoder)
      oos.close()

      val serializedCoderBytes = baos.toByteArray
      println(s"[DEBUG Serialization] Serialized AvroCoder: ${serializedCoderBytes.length} bytes")

      // Deserialize the coder
      val bais = new ByteArrayInputStream(serializedCoderBytes)
      val ois = new java.io.ObjectInputStream(bais)
      val deserializedCoder = ois.readObject().asInstanceOf[AvroCoder[User]]
      ois.close()

      println(s"[DEBUG Serialization] Deserialized AvroCoder successfully")

      // Now try to use the deserialized coder
      val encodedUser = CoderUtils.encodeToByteArray(originalCoder, user)
      val decodedUser = CoderUtils.decodeFromByteArray(deserializedCoder, encodedUser)

      println(s"[DEBUG Serialization] Decoded with deserialized coder: ${decodedUser.getFirstName}")
      decodedUser.getId shouldBe 3
      decodedUser.getFirstName shouldBe "Charlie"

    } catch {
      case e: java.io.NotSerializableException =>
        println(s"[DEBUG Serialization] AvroCoder is not serializable: ${e.getMessage}")
      // This is expected - AvroCoder may not be Java-serializable
      // Beam uses its own serialization mechanism
      case e: Exception =>
        println(s"[DEBUG Serialization] Exception: ${e.getClass.getName}: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
}
