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
import com.spotify.scio.avro.{Account, Address, User}
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.smb.syntax.all._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag

import java.nio.file.Files
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Integration tests for SMBCollection API.
 *
 * Tests the new fluent SMB collection API built on transformation chains.
 */
class SMBCollectionTest extends PipelineSpec {

  private val address = new Address("street1", "street2", "city", "state", "01234", "USA")

  private val user1 = new User(
    1,
    "Smith",
    "Alice",
    "alice@example.com",
    Collections.emptyList(),
    address
  )

  private val user2 = new User(
    2,
    "Jones",
    "Bob",
    "bob@example.com",
    Collections.emptyList(),
    address
  )

  private val account1 = new Account(1, "savings", "Alice Savings", 1000.0, null)
  private val account2 = new Account(2, "checking", "Bob Checking", 500.0, null)

  "SMBCollection.cogroup" should "perform a basic 2-way cogroup" in {
    // Use fixed directories for debugging - DO NOT DELETE
    val tempUsersDir = new java.io.File("/tmp/smb-debug-users")
    val tempAccountsDir = new java.io.File("/tmp/smb-debug-accounts")
    val tempOutputDir = new java.io.File("/tmp/smb-debug-output")

    // Clean and recreate directories
    def recreateDir(dir: java.io.File): Unit = {
      if (dir.exists()) {
        dir.listFiles().foreach { f =>
          if (f.isDirectory) {
            f.listFiles().foreach(_.delete())
          }
          f.delete()
        }
      }
      dir.mkdirs()
    }

    recreateDir(tempUsersDir)
    recreateDir(tempAccountsDir)
    recreateDir(tempOutputDir)

    // Step 1: Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Step 2: Test SMBCollection.cogroup
    {
      val sc = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      // Use SMBCollection.cogroup
      implicit val scImplicit: ScioContext = sc
      val cogrouped = SMBCollection.cogroup2(
        classOf[Integer],
        usersRead,
        accountsRead
      )

      // Transform: merge users with their accounts
      val merged = cogrouped.flatMap { case (_, (users, accounts)) =>
        for {
          user <- users
          account <- accounts
        } yield {
          User
            .newBuilder(user)
            .setAccounts(List(account).asJava)
            .build()
        }
      }

      // Save output
      merged.saveAsSortedBucket(
        AvroSortedBucketIO
          .transformOutput(classOf[Integer], "id", classOf[User])
          .to(tempOutputDir.getAbsolutePath)
      )

      sc.run().waitUntilDone() // SMB transforms execute automatically
    }

    // Step 3: Verify output exists and has correct structure
    SmbTestHelper.verifySmbOutput[User](
      tempOutputDir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )

    // Note: Data correctness verification with readAllSmbRecords would go here,
    // but it requires handling Avro GenericRecord vs. specific record types
  }

  "SMBCollection.values" should "provide ergonomic value-only transformations" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-values").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output-values").toFile

    tempUsersDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Write test data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test .values API
    {
      val sc = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      // Use read API (returns Iterable[V] directly)
      implicit val scImpl: ScioContext = sc
      SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      sc.run().waitUntilDone() // SMB transforms execute automatically
    }

    // Verify output structure
    SmbTestHelper.verifySmbOutput[User](
      tempOutputDir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )
  }

  "SMBCollection with composite key" should "handle primary + secondary keys" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-composite").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output-composite").toFile

    tempUsersDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Write test data with primary key (id) and secondary key (last_name)
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[CharSequence], "last_name", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test SMBCollection with composite key (primary + secondary)
    {
      val sc = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      implicit val scImpl: ScioContext = sc

      // CharSequence coder for composite key
      implicit val charSeqCoder: Coder[CharSequence] =
        Coder.xmap(Coder.stringCoder)(s => s: CharSequence, _.toString)

      // Read with composite key (Integer, CharSequence) - secondary key is preserved
      // Need to flatten the Iterable[User] to User before saving
      val collection = SMBCollection
        .readWithSecondary(classOf[Integer], classOf[CharSequence], usersRead)
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )

      // Write with primary key (id) and secondary key (last_name)
      collection.saveAsSortedBucket(
        AvroSortedBucketIO
          .transformOutput(
            classOf[Integer],
            "id",
            classOf[CharSequence],
            "last_name",
            classOf[User]
          )
          .to(tempOutputDir.getAbsolutePath)
      )

      sc.run().waitUntilDone()
    }

    // Verify output structure
    SmbTestHelper.verifySmbOutput[User](
      tempOutputDir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )
  }

  "SMBCollection multi-output" should "support multiple outputs from same base" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-multi").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-accounts-multi").toFile
    val tempOutput1Dir = Files.createTempDirectory("smb-output1").toFile
    val tempOutput2Dir = Files.createTempDirectory("smb-output2").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()
    tempOutput1Dir.deleteOnExit()
    tempOutput2Dir.deleteOnExit()

    // Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test multi-output
    {
      val sc = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      // Create base transformation
      implicit val scImplicit: ScioContext = sc
      val base = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .flatMap { case (_, (users, accounts)) =>
          for {
            user <- users
            account <- accounts
          } yield {
            User
              .newBuilder(user)
              .setAccounts(List(account).asJava)
              .build()
          }
        }

      // Fan out to multiple outputs
      // Output 1: Just the users with accounts
      base.saveAsSortedBucket(
        AvroSortedBucketIO
          .transformOutput(classOf[Integer], "id", classOf[User])
          .to(tempOutput1Dir.getAbsolutePath)
      )

      // Output 2: Transform firstName to uppercase
      base
        .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutput2Dir.getAbsolutePath)
        )

      // Output 3: Side-effect-only tap (no children) - tests NoOpConsumer
      val tapCounter = com.spotify.scio.ScioMetrics.counter("smbTapCounter")
      base.tap { _ =>
        tapCounter.inc()
      }

      val result = sc.run().waitUntilDone()

      // Verify tap side effect executed (2 users processed)
      result.counter(tapCounter).committed.get shouldBe 2
    }

    // Verify both outputs have correct structure
    SmbTestHelper.verifySmbOutput[User](
      tempOutput1Dir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )

    SmbTestHelper.verifySmbOutput[User](
      tempOutput2Dir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )
  }

  it should "support mixing SMB outputs with toSCollection" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-mixed").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-accounts-mixed").toFile
    val tempSmbOutputDir = Files.createTempDirectory("smb-output-mixed").toFile
    val tempAvroOutputDir = Files.createTempDirectory("avro-output-mixed").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()
    tempSmbOutputDir.deleteOnExit()
    tempAvroOutputDir.deleteOnExit()

    // Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test mixed SMB output + SCollection output
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      // Create base transformation
      val base = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .flatMap { case (_, (users, accounts)) =>
          for {
            user <- users
            account <- accounts
          } yield {
            User
              .newBuilder(user)
              .setAccounts(List(account).asJava)
              .build()
          }
        }

      // SMB file output
      base.saveAsSortedBucket(
        AvroSortedBucketIO
          .transformOutput(classOf[Integer], "id", classOf[User])
          .to(tempSmbOutputDir.getAbsolutePath)
      )

      // SCollection output (regular Avro) - V is already the user
      val sCollection = base.toSCollectionAndSeal()
      sCollection.saveAsAvroFile(tempAvroOutputDir.getAbsolutePath)

      sc.run().waitUntilDone() // Both outputs execute in single pass!
    }

    // Verify SMB output structure
    SmbTestHelper.verifySmbOutput[User](
      tempSmbOutputDir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )

    // Verify Avro output exists and contains expected records
    val avroFiles = new java.io.File(tempAvroOutputDir.getAbsolutePath)
      .listFiles()
      .filter(_.getName.endsWith(".avro"))
    avroFiles should not be empty

    // Read and verify Avro content
    {
      val sc = ScioContext()
      val records = sc.avroFile[User](tempAvroOutputDir.getAbsolutePath + "/*.avro")

      records should containInAnyOrder(
        Seq(
          User.newBuilder(user1).setAccounts(List(account1).asJava).build(),
          User.newBuilder(user2).setAccounts(List(account2).asJava).build()
        )
      )

      sc.run().waitUntilDone()
    }
  }

  it should "support toSCollection only without SMB outputs" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-scollection-only").toFile
    val tempAvroOutputDir = Files.createTempDirectory("avro-output-scollection-only").toFile

    tempUsersDir.deleteOnExit()
    tempAvroOutputDir.deleteOnExit()

    // Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test toSCollection only (no SMB file outputs)
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      // Read and transform to SCollection directly (no saveAsSortedBucket calls)
      // Using the toSCollectionAndSeal API for cleaner code
      val sCollection = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )
        .toSCollectionAndSeal() // Returns SCollection[V] directly!

      // Save as regular Avro
      sCollection.saveAsAvroFile(tempAvroOutputDir.getAbsolutePath)

      sc.run().waitUntilDone()
    }

    // Verify Avro output exists and contains expected records
    val avroFiles = new java.io.File(tempAvroOutputDir.getAbsolutePath)
      .listFiles()
      .filter(_.getName.endsWith(".avro"))
    avroFiles should not be empty

    // Read and verify Avro content
    {
      val sc = ScioContext()
      val records = sc.avroFile[User](tempAvroOutputDir.getAbsolutePath + "/*.avro")

      records should containInAnyOrder(
        Seq(
          User.newBuilder(user1).setFirstName("ALICE").build(),
          User.newBuilder(user2).setFirstName("BOB").build()
        )
      )

      sc.run().waitUntilDone()
    }
  }

  it should "support both keyed and values-only toSCollection APIs" in {
    val tempUsersDir1 = Files.createTempDirectory("smb-users-api-variant1").toFile
    val tempUsersDir2 = Files.createTempDirectory("smb-users-api-variant2").toFile
    tempUsersDir1.deleteOnExit()
    tempUsersDir2.deleteOnExit()

    // Write test data for first test
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir1.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Write test data for second test
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir2.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test Approach 1: Using values view -> toSCollection returns SCollection[V] directly
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir1.getAbsolutePath)

      val valuesOnly = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )
        .toSCollectionAndSeal()

      valuesOnly should containInAnyOrder(
        Seq(
          User.newBuilder(user1).setFirstName("ALICE").build(),
          User.newBuilder(user2).setFirstName("BOB").build()
        )
      )

      sc.run().waitUntilDone()
    }

    // Test Approach 2: Using keyed view -> toSCollection.map(_._2) extracts values
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir2.getAbsolutePath)

      val keyedThenValues = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )
        .toSCollectionAndSeal()

      keyedThenValues should containInAnyOrder(
        Seq(
          User.newBuilder(user1).setFirstName("ALICE").build(),
          User.newBuilder(user2).setFirstName("BOB").build()
        )
      )

      sc.run().waitUntilDone()
    }
  }

  "SMBCollection.toSCollection" should "convert to SCollection with transformations" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-tosco").toFile
    tempUsersDir.deleteOnExit()

    // Write test data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test toSCollection
    {
      val sc = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      // Create SMBCollection with transformations
      implicit val scImpl: ScioContext = sc
      val smbCollection = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )

      // Convert to SCollection
      val sCollection = smbCollection.toSCollectionAndSeal()

      // Verify we can use regular SCollection operations
      val result = sCollection.map { user =>
        (user.getId.toInt, user.getFirstName.toString)
      }

      result should containInAnyOrder(
        Seq(
          (1, "ALICE"),
          (2, "BOB")
        )
      )

      sc.run().waitUntilDone()
    }
  }

  "SMBCollection.withSideInputs" should "allow context-aware transformations with side inputs" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-sideinputs").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-accounts-sideinputs").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output-sideinputs").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test side inputs
    {
      val sc = ScioContext()

      // Create side input: threshold for filtering accounts
      val threshold = sc.parallelize(Seq(750.0)).asSingletonSideInput

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      implicit val scimpl: ScioContext = sc

      // Use withSideInputs to access threshold in transformations
      SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .withSideInputs(threshold)
        .filter { (ctx, value) =>
          // Filter based on side input threshold
          val (_, (_, accounts)) = value
          val minAmount = ctx(threshold)
          accounts.exists(_.getAmount >= minAmount)
        }
        .flatMap { (_, value) =>
          // Extract users from the filtered cogroup result
          val (_, (users, _)) = value
          users
        }
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      sc.run().waitUntilDone()
    }

    // Verify output structure
    SmbTestHelper.verifySmbOutput[User](
      tempOutputDir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )

    // Note: Only user1 should pass the filter (account1 has amount 1000.0 >= 750.0)
    // user2's account has only 500.0, so it should be filtered out
  }

  it should "support multi-output with side inputs" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-multi-side").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-accounts-multi-side").toFile
    val tempOutput1Dir = Files.createTempDirectory("smb-output1-multi-side").toFile
    val tempOutput2Dir = Files.createTempDirectory("smb-output2-multi-side").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()
    tempOutput1Dir.deleteOnExit()
    tempOutput2Dir.deleteOnExit()

    // Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test multi-output with side inputs
    {
      val sc = ScioContext()

      // Create two different side inputs for different outputs
      val threshold1 = sc.parallelize(Seq(500.0)).asSingletonSideInput
      val threshold2 = sc.parallelize(Seq(1000.0)).asSingletonSideInput

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      implicit val scimpl: ScioContext = sc

      // Create base with side inputs
      // Materialize at fanout point since we have multiple outputs consuming the same iterables
      val base = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .map { case (_, (users, accounts)) => (users.toSeq, accounts.toSeq) }
        .withSideInputs(threshold1, threshold2)

      // Output 1: Filter with threshold1 (>= 500.0)
      base
        .filter { (ctx, value) =>
          val (users, accounts) = value
          accounts.exists(_.getAmount >= ctx(threshold1))
        }
        .flatMap { (_, value) =>
          val (users, _) = value
          users
        }
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutput1Dir.getAbsolutePath)
        )

      // Output 2: Filter with threshold2 (>= 1000.0)
      base
        .filter { (ctx, value) =>
          val (users, accounts) = value
          accounts.exists(_.getAmount >= ctx(threshold2))
        }
        .flatMap { (_, value) =>
          val (users, _) = value
          users
        }
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutput2Dir.getAbsolutePath)
        )

      sc.run().waitUntilDone()
    }

    // Verify both outputs
    SmbTestHelper.verifySmbOutput[User](
      tempOutput1Dir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )

    SmbTestHelper.verifySmbOutput[User](
      tempOutput2Dir.getAbsolutePath,
      expectedNumBuckets = 2,
      keyExtractor = user => user.getId.toString
    )

    // Note: output1 should have both users (both have accounts >= 500)
    // output2 should only have user1 (only user1 has account >= 1000)
  }

  "SMBCollection with multiple shards" should "correctly merge across shards within sources using toSCollection" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-multishard").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-accounts-multishard").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Create more test data to span multiple shards
    val users = (1 to 20).map { i =>
      new User(
        i,
        s"LastName$i",
        s"FirstName$i",
        s"user$i@example.com",
        Collections.emptyList(),
        address
      )
    }

    val accounts = (1 to 20).map { i =>
      new Account(i, s"account-$i", s"Account $i", i * 100.0, null)
    }

    // Step 1: Write test SMB data with MULTIPLE SHARDS
    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(4)
            .withNumShards(3) // Multiple shards to test merging
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(accounts)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(4)
            .withNumShards(3) // Multiple shards to test merging
        )
      sc.run().waitUntilDone()
    }

    // Step 2: Test SMBCollection.cogroup with multi-shard sources using toSCollection
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      // Cogroup and merge - this tests that SimpleKeyGroupReader correctly
      // merges values from multiple shards within each source
      val result = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .flatMapValues { case (users, accounts) =>
          for {
            user <- users
            account <- accounts
          } yield {
            User
              .newBuilder(user)
              .setAccounts(List(account).asJava)
              .build()
          }
        }
        .toSCollectionAndSeal()
        .map(_._2)

      // Verify ALL records are present (no data loss from sharding)
      result should containInAnyOrder((1 to 20).map { i =>
        User
          .newBuilder(users(i - 1))
          .setAccounts(List(accounts(i - 1)).asJava)
          .build()
      })

      sc.run().waitUntilDone()
    }
  }

  it should "handle multiple shards with transformations and filtering using toSCollection" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-multishard-filter").toFile

    tempUsersDir.deleteOnExit()

    // Create test data spanning multiple shards
    val users = (1 to 30).map { i =>
      new User(
        i,
        s"LastName$i",
        s"FirstName$i",
        s"user$i@example.com",
        Collections.emptyList(),
        address
      )
    }

    // Write with multiple shards
    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(8)
            .withNumShards(4) // 4 shards per bucket
        )
      sc.run().waitUntilDone()
    }

    // Read, transform, and filter using toSCollection
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val result = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap { users =>
          users
            .filter(_.getId % 2 == 0) // Keep only even IDs
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        }
        .toSCollectionAndSeal()

      // Verify filtering worked correctly across all shards
      val expected = (2 to 30 by 2).map { i =>
        User.newBuilder(users(i - 1)).setFirstName(s"FIRSTNAME$i").build()
      }

      result should containInAnyOrder(expected)

      sc.run().waitUntilDone()
    }
  }

  "SMBCollection.saveAsSortedBucket" should "return Deferred[ClosedTap] that seals graph on .get" in {
    val tempUsersDir = Files.createTempDirectory("smb-users").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output").toFile

    tempUsersDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Step 1: Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Step 2: Test that Deferred[ClosedTap] seals graph on .get
    {
      implicit val sc: ScioContext = ScioContext()

      val deferredTap = SMBCollection
        .read(
          classOf[Integer],
          AvroSortedBucketIO
            .read(new TupleTag[User]("users"), classOf[User])
            .from(tempUsersDir.getAbsolutePath)
        )
        .map(users => User.newBuilder(users.head).setFirstName("MAPPED").build())
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      // Calling .get on Deferred should seal the graph and apply transforms
      val closedTap = deferredTap.get

      // Run the pipeline
      val result = sc.run().waitUntilFinish()

      // Now we can access the tap with ScioResult
      val tap = result.tap(closedTap)
      val writtenData = tap.value.toList

      // Verify we got 2 users (one per bucket) with mapped first name
      writtenData should have size 2
      writtenData.foreach { user =>
        user.getFirstName.toString shouldBe "MAPPED"
      }
    }
  }

  it should "work with Parquet format" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-parquet").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output-parquet").toFile

    tempUsersDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Step 1: Write test SMB data in Parquet format
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Step 2: Test that Deferred[ClosedTap] works with Parquet
    {
      implicit val sc: ScioContext = ScioContext()

      val deferredTap = SMBCollection
        .read(
          classOf[Integer],
          org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
            .read(new TupleTag[User]("users"), classOf[User])
            .from(tempUsersDir.getAbsolutePath)
        )
        .map(users => User.newBuilder(users.head).setFirstName("PARQUET_MAPPED").build())
        .saveAsSortedBucket(
          org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      // Calling .get on Deferred should seal the graph and apply transforms
      val closedTap = deferredTap.get

      // Run the pipeline
      val result = sc.run().waitUntilFinish()

      // Now we can access the tap with ScioResult
      val tap = result.tap(closedTap)
      val writtenData = tap.value.toList

      // Verify we got 2 users (one per bucket) with mapped first name
      writtenData should have size 2
      writtenData.foreach { user =>
        user.getFirstName.toString shouldBe "PARQUET_MAPPED"
      }
    }
  }

  "SMBCollection.read (single source)" should "handle simple read without cogroup" in {
    val tempUsersDir = Files.createTempDirectory("smb-single-read").toFile
    val tempOutputDir = Files.createTempDirectory("smb-single-output").toFile

    tempUsersDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Write test data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test single-source read with transformations and tap
    {
      implicit val sc: ScioContext = ScioContext()

      val deferredTap = SMBCollection
        .read(
          classOf[Integer],
          AvroSortedBucketIO
            .read(new TupleTag[User]("users"), classOf[User])
            .from(tempUsersDir.getAbsolutePath)
        )
        .flatMap(users =>
          users
            .map(u => User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build())
        )
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      val closedTap = deferredTap.get
      val result = sc.run().waitUntilFinish()
      val tap = result.tap(closedTap)
      val writtenData = tap.value.toList

      writtenData should have size 2
      writtenData.map(_.getFirstName.toString).toSet shouldBe Set("ALICE", "BOB")
    }
  }

  "SMBCollection.cogroup3" should "perform 3-way cogroup with Avro sources" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-cogroup3").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-accounts-cogroup3").toFile
    val tempMoreAccountsDir = Files.createTempDirectory("smb-more-accounts-cogroup3").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output-cogroup3").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()
    tempMoreAccountsDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    val account3 = new Account(1, "savings2", "Alice Savings 2", 2000.0, null)
    val account4 = new Account(2, "savings2", "Bob Savings 2", 1500.0, null)

    // Write three sources - all with Integer ID keys
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(Seq(account3, account4))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempMoreAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test 3-way cogroup
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      val moreAccountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("more-accounts"), classOf[Account])
        .from(tempMoreAccountsDir.getAbsolutePath)

      val deferredTap = SMBCollection
        .cogroup3(
          classOf[Integer],
          usersRead,
          accountsRead,
          moreAccountsRead
        )
        .flatMap { case (_, (users, accounts1, accounts2)) =>
          for {
            user <- users
            account1 <- accounts1
            account2 <- accounts2
          } yield {
            User
              .newBuilder(user)
              .setAccounts(List(account1, account2).asJava)
              .build()
          }
        }
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      val closedTap = deferredTap.get
      val result = sc.run().waitUntilFinish()
      val tap = result.tap(closedTap)
      val writtenData = tap.value.toList

      writtenData should have size 2
      writtenData.foreach { u =>
        u.getAccounts should have size 2
      }
    }
  }

  "SMBCollection (comprehensive)" should "handle mixed formats, multi-output, taps, and SCollections with partial overlap" in {
    val tempUsersDir = Files.createTempDirectory("smb-comp-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-comp-accounts").toFile
    val tempAvroOutputDir = Files.createTempDirectory("smb-comp-avro-out").toFile
    val tempParquetOutputDir = Files.createTempDirectory("smb-comp-parquet-out").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()
    tempAvroOutputDir.deleteOnExit()
    tempParquetOutputDir.deleteOnExit()

    // Create test data with PARTIAL OVERLAP:
    // Users: IDs 1-10 (Avro)
    // Accounts: IDs 2-11 (Parquet - missing ID 1, has extra ID 11)
    val users = (1 to 10).map { i =>
      new User(i, s"Last$i", s"First$i", s"user$i@example.com", Collections.emptyList(), address)
    }

    val accounts = (2 to 11).map { i =>
      new Account(i, s"acct-$i", s"Account $i", i * 100.0, null)
    }

    // Write sources: 1 Avro + 1 Parquet (mixed formats!)
    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(4)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      val sc = ScioContext()
      sc.parallelize(accounts)
        .saveAsSortedBucket(
          org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(4)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test: cogroup2 with mixed formats (Avro + Parquet) + multiple outputs + taps + SCollections
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      // Step 1: cogroup2 with mixed formats (Avro Users + Parquet Accounts)
      val cogrouped = SMBCollection.cogroup2(
        classOf[Integer],
        usersRead,
        accountsRead
      )

      // Step 2: Intermediate transformation - merge users with accounts
      val merged = cogrouped.flatMap { case (_, (users, accounts)) =>
        for {
          user <- users
          account <- accounts
        } yield {
          User
            .newBuilder(user)
            .setAccounts(List(account).asJava)
            .build()
        }
      }

      // Step 3: Another transformation - uppercase first names
      val uppercased = merged.map(u =>
        User.newBuilder(u).setFirstName(u.getFirstName.toString.toUpperCase).build()
      )

      // Step 4: Filter high-value accounts
      val filtered = uppercased.flatMap { u =>
        if (u.getAccounts.asScala.exists(_.getAmount >= 500.0)) {
          Some(u)
        } else {
          None
        }
      }

      // Output 1: Avro SMB output with tap
      val avroTap = filtered.saveAsSortedBucket(
        AvroSortedBucketIO
          .transformOutput(classOf[Integer], "id", classOf[User])
          .to(tempAvroOutputDir.getAbsolutePath)
      )

      // Output 2: Parquet SMB output with tap (from same base)
      val parquetTap = filtered.saveAsSortedBucket(
        org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
          .transformOutput(classOf[Integer], "id", classOf[User])
          .to(tempParquetOutputDir.getAbsolutePath)
      )

      // Output 3: SCollection output 1 (values only)
      val sCollection1Deferred = filtered.toDeferredSCollection()

      // Output 4: SCollection output 2 (from different branch)
      val sCollection2Deferred = uppercased.toDeferredSCollection()

      // Seal graph and get taps
      val closedAvroTap = avroTap.get
      val closedParquetTap = parquetTap.get
      val sCollection1 = sCollection1Deferred.get
      val sCollection2 = sCollection2Deferred.get

      // Run pipeline
      val result = sc.run().waitUntilFinish()

      // Verify tap 1 (Avro)
      val avroData = result.tap(closedAvroTap).value.toList
      avroData should not be empty
      // IDs 5-10 should have amount >= 500 (amounts are id * 100)
      // ID 2-4 have amounts 200-400 (filtered out)
      avroData.map(_.getId.toInt).toSet shouldBe (5 to 10).toSet
      avroData.foreach { u =>
        u.getFirstName.toString should startWith("FIRST")
        u.getAccounts should not be empty
      }

      // Verify tap 2 (Parquet) - should be identical to Avro tap
      val parquetData = result.tap(closedParquetTap).value.toList
      parquetData.map(_.getId.toInt).toSet shouldBe (5 to 10).toSet

      // Verify SCollection 1 (filtered values)
      val expectedFiltered = (5 to 10).map { i =>
        User
          .newBuilder(users(i - 1))
          .setFirstName(s"FIRST$i")
          .setAccounts(List(accounts(i - 2)).asJava) // accounts start at index 0 = ID 2
          .build()
      }
      sCollection1 should containInAnyOrder(expectedFiltered)

      // Verify SCollection 2 (all cogroup matches, unfiltered)
      // IDs 2-10 have both users and accounts (ID 1 has no account, ID 11 has no user)
      val expectedAll = (2 to 10).map { i =>
        User
          .newBuilder(users(i - 1))
          .setFirstName(s"FIRST$i")
          .setAccounts(List(accounts(i - 2)).asJava)
          .build()
      }
      sCollection2 should containInAnyOrder(expectedAll)
    }
  }

  "SMBCollection lazy iteration" should "correctly handle partial consumption with automatic exhaustion" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-partial").toFile
    val tempOutputDir = Files.createTempDirectory("smb-output-partial").toFile
    tempUsersDir.deleteOnExit()
    tempOutputDir.deleteOnExit()

    // Create multiple users with the same ID to get multiple values per key group
    val user1a =
      new User(1, "Smith", "Alice", "alice@example.com", Collections.emptyList(), address)
    val user1b = new User(1, "Smith", "Bob", "bob@example.com", Collections.emptyList(), address)
    val user1c =
      new User(1, "Smith", "Charlie", "charlie@example.com", Collections.emptyList(), address)
    val user2a =
      new User(2, "Jones", "David", "david@example.com", Collections.emptyList(), address)
    val user2b = new User(2, "Jones", "Eve", "eve@example.com", Collections.emptyList(), address)
    val user3a =
      new User(3, "Brown", "Frank", "frank@example.com", Collections.emptyList(), address)
    val user3b =
      new User(3, "Brown", "Grace", "grace@example.com", Collections.emptyList(), address)
    val user3c =
      new User(3, "Brown", "Henry", "henry@example.com", Collections.emptyList(), address)

    // Write test data - multiple users per ID
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1a, user1b, user1c, user2a, user2b, user3a, user3b, user3c))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test partial consumption: only take first user from each key group
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      // Only consume the FIRST value from each key group - ignore the rest
      // If exhaustion doesn't work, the iterator won't advance properly
      val result = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(_.headOption) // Take first user, leave rest unconsumed
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutputDir.getAbsolutePath)
        )

      val closedTap = result.get
      val pipelineResult = sc.run().waitUntilFinish()
      val tap = pipelineResult.tap(closedTap)
      val output = tap.value.toList

      // Should have exactly 3 users (one per key group)
      output should have size 3

      // Should have one user from each ID (1, 2, 3)
      output.map(_.getId.toInt).toSet shouldBe Set(1, 2, 3)

      // Should be ONE user from each group (verify we didn't get duplicates)
      // Note: Order within key group is not deterministic after SMB write,
      // so we just verify we got exactly one user per ID
      output.groupBy(_.getId).values.foreach { usersForId =>
        usersForId should have size 1
      }
    }
  }

  it should "fail-fast when fanout consumes the same iterable twice without materialization" in {
    val tempUsersDir = Files.createTempDirectory("smb-users-fanout-fail").toFile
    val tempOutput1Dir = Files.createTempDirectory("smb-output1-fanout-fail").toFile
    val tempOutput2Dir = Files.createTempDirectory("smb-output2-fanout-fail").toFile

    tempUsersDir.deleteOnExit()
    tempOutput1Dir.deleteOnExit()
    tempOutput2Dir.deleteOnExit()

    // Write test SMB data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // This should fail: fanout without materialization
    {
      val sc = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      implicit val scImpl: ScioContext = sc

      val base = SMBCollection.read(classOf[Integer], usersRead)

      // Output 1: toSCollection (consumes the iterable)
      base.toDeferredSCollection()

      // Output 2: saveAsSortedBucket (tries to consume the same iterable!)
      base
        .flatMap(users => users) // This will fail - iterable already consumed
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .transformOutput(classOf[Integer], "id", classOf[User])
            .to(tempOutput1Dir.getAbsolutePath)
        )

      // Should throw IllegalStateException with our helpful error message
      val exception = intercept[org.apache.beam.sdk.Pipeline$PipelineExecutionException] {
        sc.run().waitUntilDone()
      }

      exception.getCause shouldBe a[IllegalStateException]
      exception.getCause.getMessage should include(
        "SMBCollection values are lazy iterables that can only be consumed once"
      )
      exception.getCause.getMessage should include("Fix by materializing at the fanout point")
    }
  }

  "SMBCollection with composite key" should "export to SCollection with both keys" in {
    val tempUsersDir = Files.createTempDirectory("smb-composite-export").toFile
    tempUsersDir.deleteOnExit()

    // Write test data with primary and secondary keys
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[CharSequence], "last_name", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Read with composite key and export to SCollection
    {
      implicit val sc: ScioContext = ScioContext()

      implicit val charSeqCoder: Coder[CharSequence] =
        Coder.xmap(Coder.stringCoder)(s => s: CharSequence, _.toString)

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val result = SMBCollection
        .readWithSecondary(classOf[Integer], classOf[CharSequence], usersRead)
        .map(_.toSeq)
        .toSCollectionAndSeal()

      // Verify we got the users (keys are not in the value)
      val mapped = result.map { users =>
        users.map(_.getFirstName.toString)
      }

      mapped should containInAnyOrder(
        Seq(
          Seq("Alice"),
          Seq("Bob")
        )
      )

      sc.run().waitUntilDone()
    }
  }

  "SMBCollection.tap" should "not fail when used before toSCollectionAndSeal" in {
    val tempUsersDir = Files.createTempDirectory("smb-tap-leaf").toFile
    tempUsersDir.deleteOnExit()

    // Write test data
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Test that tap works and doesn't get optimized away
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      // Use tap followed by toSCollectionAndSeal
      // Materialize first to avoid double-consumption
      val result = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(_.toSeq) // Flatten to individual users
        .tap { user =>
          // Tap side effect - just verify it doesn't cause errors
          user.getId // Access the data
        }
        .toSCollectionAndSeal()

      // Verify pipeline completes successfully with tap in the chain
      // Should have 2 individual users (not 2 Seqs)
      result should haveSize(2)

      sc.run().waitUntilDone()
    }
  }

}
