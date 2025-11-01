/*
 * Copyright 2025 Spotify AB
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

package com.spotify.scio.smb

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, SortedBucketIO}
import org.apache.beam.sdk.values.TupleTag

import java.io.File
import java.nio.file.Files
import scala.collection.mutable
import scala.util.Try

/**
 * Test context for easily setting up SMBCollection tests.
 *
 * Manages temp directories, writes test data as SMB files, and provides convenient accessors for
 * reading in tests.
 *
 * Example usage:
 * {{{
 * "SMBCollection" should "cogroup users and accounts" in {
 *   SMBTestContext()
 *     .avroInput("users", classOf[Integer], "id", Seq(user1, user2))
 *     .avroInput("accounts", classOf[Integer], "id", Seq(account1, account2))
 *     .run { ctx =>
 *       val sc = ScioContext()
 *       val result = SMBCollection
 *         .cogroup2(
 *           SMBKey.primary(classOf[Integer]),
 *           ctx.avroRead[User]("users"),
 *           ctx.avroRead[Account]("accounts")
 *         )
 *         .toSCollectionAndSeal()
 *
 *       result should containInAnyOrder(expected)
 *       sc.run()
 *     }
 * }
 * }}}
 */
class SMBTestContext private (
  private val inputs: mutable.Map[String, SMBTestInput[_]],
  private val tempDirs: mutable.Set[File],
  private val numBuckets: Int,
  private val numShards: Int
) {

  /**
   * Register an Avro input dataset.
   *
   * @param name
   *   Logical name for this input (used in avroRead)
   * @param keyClass
   *   Key class for bucketing
   * @param keyField
   *   Name of the key field in the record
   * @param data
   *   Test data to write
   * @tparam K
   *   Key type
   * @tparam T
   *   Record type
   */
  def avroInput[K, T <: org.apache.avro.specific.SpecificRecord](
    name: String,
    keyClass: Class[K],
    keyField: String,
    data: Seq[T]
  )(implicit coder: Coder[T]): SMBTestContext = {
    inputs(name) = new AvroTestInput[K, T](keyClass, keyField, data, numBuckets, numShards)
    this
  }

  /**
   * Register an Avro input dataset with secondary key.
   *
   * @param name
   *   Logical name for this input
   * @param keyClass
   *   Primary key class
   * @param keyField
   *   Primary key field name
   * @param keyClassSecondary
   *   Secondary key class
   * @param keyFieldSecondary
   *   Secondary key field name
   * @param data
   *   Test data to write
   * @tparam K1
   *   Primary key type
   * @tparam K2
   *   Secondary key type
   * @tparam T
   *   Record type
   */
  def avroInputWithSecondaryKey[K1, K2, T <: org.apache.avro.specific.SpecificRecord](
    name: String,
    keyClass: Class[K1],
    keyField: String,
    keyClassSecondary: Class[K2],
    keyFieldSecondary: String,
    data: Seq[T]
  )(implicit coder: Coder[T]): SMBTestContext = {
    inputs(name) = new AvroTestInputWithSecondaryKey[K1, K2, T](
      keyClass,
      keyField,
      keyClassSecondary,
      keyFieldSecondary,
      data,
      numBuckets,
      numShards
    )
    this
  }

  /**
   * Create a SortedBucketIO.Read for an Avro input.
   *
   * @param name
   *   Logical name of the input (registered with avroInput)
   * @tparam T
   *   Record type
   */
  def avroRead[T <: org.apache.avro.specific.SpecificRecord](
    name: String
  )(implicit ct: scala.reflect.ClassTag[T]): SortedBucketIO.Read[T] = {
    val input = inputs.getOrElse(
      name,
      throw new IllegalArgumentException(s"No input registered with name: $name")
    )
    val path = input.getPath
    val tupleTag = new TupleTag[T](name)
    val recordClass = ct.runtimeClass.asInstanceOf[Class[T]]
    AvroSortedBucketIO.read(tupleTag, recordClass).from(path)
  }

  /**
   * Get the file path for an input (useful for custom read configurations).
   *
   * @param name
   *   Logical name of the input
   */
  def pathFor(name: String): String =
    inputs
      .getOrElse(name, throw new IllegalArgumentException(s"No input registered with name: $name"))
      .getPath

  /**
   * Run the test with this context.
   *
   * Writes all registered inputs to temp directories, runs the test block, then cleans up temp
   * directories.
   *
   * @param testBlock
   *   Test code to execute
   * @tparam T
   *   Return type of test block
   */
  def run[T](testBlock: SMBTestContext => T): T = {
    try {
      // Write all inputs
      inputs.values.foreach(_.writeData(this))

      // Run test
      testBlock(this)
    } finally {
      // Cleanup temp directories
      tempDirs.foreach { dir =>
        Try(deleteRecursively(dir))
      }
    }
  }

  /**
   * Create a temp directory and register it for cleanup.
   *
   * @param prefix
   *   Directory name prefix
   */
  private[smb] def createTempDir(prefix: String): File = {
    val dir = Files.createTempDirectory(prefix).toFile
    tempDirs += dir
    dir
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}

object SMBTestContext {

  /**
   * Create a new SMBTestContext with default bucket/shard configuration.
   *
   * @param numBuckets
   *   Number of buckets for SMB outputs (default: 2)
   * @param numShards
   *   Number of shards per bucket (default: 1)
   */
  def apply(numBuckets: Int = 2, numShards: Int = 1): SMBTestContext =
    new SMBTestContext(mutable.Map.empty, mutable.Set.empty, numBuckets, numShards)
}

/** Base trait for test inputs that write data to SMB format. */
sealed private trait SMBTestInput[T] {
  def getPath: String
  def writeData(ctx: SMBTestContext): Unit
}

/** Avro test input implementation. */
private class AvroTestInput[K, T <: org.apache.avro.specific.SpecificRecord](
  keyClass: Class[K],
  keyField: String,
  data: Seq[T],
  numBuckets: Int,
  numShards: Int
)(implicit coder: Coder[T])
    extends SMBTestInput[T] {

  @volatile private var path: String = _

  override def getPath: String = {
    if (path == null) {
      throw new IllegalStateException("writeData() must be called before getPath()")
    }
    path
  }

  override def writeData(ctx: SMBTestContext): Unit = {
    val tempDir = ctx.createTempDir("smb-test-input")
    path = tempDir.getAbsolutePath

    val sc = ScioContext()
    sc.parallelize(data)
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(keyClass, keyField, data.head.getClass.asInstanceOf[Class[T]])
          .to(path)
          .withNumBuckets(numBuckets)
          .withNumShards(numShards)
      )
    sc.run().waitUntilDone()
  }
}

/** Avro test input with secondary key implementation. */
private class AvroTestInputWithSecondaryKey[K1, K2, T <: org.apache.avro.specific.SpecificRecord](
  keyClass: Class[K1],
  keyField: String,
  keyClassSecondary: Class[K2],
  keyFieldSecondary: String,
  data: Seq[T],
  numBuckets: Int,
  numShards: Int
)(implicit coder: Coder[T])
    extends SMBTestInput[T] {

  @volatile private var path: String = _

  override def getPath: String = {
    if (path == null) {
      throw new IllegalStateException("writeData() must be called before getPath()")
    }
    path
  }

  override def writeData(ctx: SMBTestContext): Unit = {
    val tempDir = ctx.createTempDir("smb-test-input-secondary")
    path = tempDir.getAbsolutePath

    val sc = ScioContext()
    sc.parallelize(data)
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(
            keyClass,
            keyField,
            keyClassSecondary,
            keyFieldSecondary,
            data.head.getClass.asInstanceOf[Class[T]]
          )
          .to(path)
          .withNumBuckets(numBuckets)
          .withNumShards(numShards)
      )
    sc.run().waitUntilDone()
  }
}
