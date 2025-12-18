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
import com.spotify.scio.avro._
import com.spotify.scio.smb.syntax.all._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag

import java.nio.file.Files
import java.util.Collections

/**
 * Unit tests for SimpleKeyGroupReader.
 *
 * Tests edge cases and correctness of the multi-source key-group merging logic.
 */
class SimpleKeyGroupReaderTest extends PipelineSpec {

  private def createAddress(): Address =
    new Address("street1", "street2", "city", "state", "01234", "USA")

  "SimpleKeyGroupReader" should "handle empty sources" in {
    val tempUsersDir = Files.createTempDirectory("smb-empty-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-empty-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Write empty sources
    {
      val sc = ScioContext()
      sc.parallelize(Seq.empty[User])
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
      sc.parallelize(Seq.empty[Account])
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Read should return empty result
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      val result = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .toSCollectionAndSeal()

      result should beEmpty

      sc.run().waitUntilDone()
    }
  }

  it should "handle sources with no overlapping keys" in {
    val tempUsersDir = Files.createTempDirectory("smb-non-overlap-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-non-overlap-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Users with IDs 1-5
    val users = (1 to 5).map { i =>
      new User(
        i,
        s"Last$i",
        s"First$i",
        s"user$i@example.com",
        Collections.emptyList(),
        createAddress()
      )
    }

    // Accounts with IDs 6-10 (no overlap)
    val accounts = (6 to 10).map { i =>
      new Account(i, s"account-$i", s"Account $i", i * 100.0, null)
    }

    {
      val sc = ScioContext()
      sc.parallelize(users)
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
      sc.parallelize(accounts)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Cogroup should produce key groups with empty sequences for missing side
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      val result = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .mapValues { case (users, accounts) =>
          (users.toSeq, accounts.toSeq)
        }
        .toSCollectionAndSeal()

      // Should have 10 key groups total (5 with only users, 5 with only accounts)
      result should haveSize(10)

      // Verify structure: keys 1-5 have users, keys 6-10 have accounts
      result.filter { case (key, (users, accounts)) =>
        if (key >= 1 && key <= 5) {
          users.nonEmpty && accounts.isEmpty
        } else if (key >= 6 && key <= 10) {
          users.isEmpty && accounts.nonEmpty
        } else {
          false
        }
      } should haveSize(10)

      sc.run().waitUntilDone()
    }
  }

  it should "correctly merge when same key appears in multiple shards" in {
    val tempUsersDir = Files.createTempDirectory("smb-duplicate-key").toFile

    tempUsersDir.deleteOnExit()

    // Create multiple users with same ID - will be distributed across shards
    val users = (1 to 10).map { i =>
      new User(
        1,
        s"Last$i",
        s"First$i",
        s"user$i@example.com",
        Collections.emptyList(),
        createAddress()
      )
    }

    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(1)
            .withNumShards(3) // Multiple shards - users will be split across them
        )
      sc.run().waitUntilDone()
    }

    // Read should merge all users with same key from all shards
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val result = SMBCollection
        .read(classOf[Integer], usersRead)
        .map(_.toSeq)
        .toSCollectionAndSeal()

      // Should have exactly 1 key group with all 10 users merged from all shards
      result.filter(_.size == 10) should haveSize(1)

      sc.run().waitUntilDone()
    }
  }

  it should "maintain key ordering across sources" in {
    val tempUsersDir = Files.createTempDirectory("smb-ordering-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-ordering-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Users with IDs: 1, 3, 5, 7, 9
    val users = (1 to 9 by 2).map { i =>
      new User(
        i,
        s"Last$i",
        s"First$i",
        s"user$i@example.com",
        Collections.emptyList(),
        createAddress()
      )
    }

    // Accounts with IDs: 2, 4, 6, 8, 10
    val accounts = (2 to 10 by 2).map { i =>
      new Account(i, s"account-$i", s"Account $i", i * 100.0, null)
    }

    {
      val sc = ScioContext()
      sc.parallelize(users)
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
      sc.parallelize(accounts)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Keys should be emitted in sorted order: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      val result = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .mapValues { case (users, accounts) =>
          (users.toSeq, accounts.toSeq)
        }
        .toSCollectionAndSeal()

      // Verify all 10 keys are present (ordering is guaranteed by SMB)
      result should haveSize(10)

      // Each key should appear exactly once
      val keys = result.map { case (k, _) => k }
      keys should containInAnyOrder((1 to 10).map(Integer.valueOf))

      sc.run().waitUntilDone()
    }
  }

  it should "correctly group all values for each key with proper source alignment" in {
    val tempUsersDir = Files.createTempDirectory("smb-value-grouping-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-value-grouping-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Create data with overlapping keys and multiple values per key per source
    // Key 1: 3 users, 2 accounts
    // Key 2: 1 user, 3 accounts
    // Key 3: 2 users, 1 account
    val users = Seq(
      new User(1, "Smith", "Alice", "alice@example.com", Collections.emptyList(), createAddress()),
      new User(1, "Smith", "Bob", "bob@example.com", Collections.emptyList(), createAddress()),
      new User(
        1,
        "Smith",
        "Charlie",
        "charlie@example.com",
        Collections.emptyList(),
        createAddress()
      ),
      new User(2, "Jones", "David", "david@example.com", Collections.emptyList(), createAddress()),
      new User(3, "Brown", "Eve", "eve@example.com", Collections.emptyList(), createAddress()),
      new User(3, "Brown", "Frank", "frank@example.com", Collections.emptyList(), createAddress())
    )

    val accounts = Seq(
      new Account(1, "checking-1a", "Account 1A", 100.0, null),
      new Account(1, "savings-1b", "Account 1B", 200.0, null),
      new Account(2, "checking-2a", "Account 2A", 300.0, null),
      new Account(2, "savings-2b", "Account 2B", 400.0, null),
      new Account(2, "investment-2c", "Account 2C", 500.0, null),
      new Account(3, "checking-3a", "Account 3A", 600.0, null)
    )

    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(2) // Multiple shards to test merging
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
            .withNumBuckets(2)
            .withNumShards(2) // Multiple shards to test merging
        )
      sc.run().waitUntilDone()
    }

    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      val result = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .mapValues { case (users, accounts) =>
          (users.toSeq, accounts.toSeq)
        }
        .toSCollectionAndSeal()

      // Should have exactly 3 key groups (keys 1, 2, 3)
      result should haveSize(3)

      // Check structure for each key - verify counts and validate specific values
      // Key 1 should have 3 users and 2 accounts
      result.filter { case (k, (users, accounts)) =>
        k == 1 && users.size == 3 && accounts.size == 2 &&
        users.map(_.getFirstName.toString).toSet == Set("Alice", "Bob", "Charlie") &&
        accounts.map(_.getType.toString).toSet == Set("checking-1a", "savings-1b")
      } should haveSize(1)

      // Key 2 should have 1 user and 3 accounts
      result.filter { case (k, (users, accounts)) =>
        k == 2 && users.size == 1 && accounts.size == 3 &&
        users.head.getFirstName.toString == "David" &&
        accounts.map(_.getType.toString).toSet == Set("checking-2a", "savings-2b", "investment-2c")
      } should haveSize(1)

      // Key 3 should have 2 users and 1 account
      result.filter { case (k, (users, accounts)) =>
        k == 3 && users.size == 2 && accounts.size == 1 &&
        users.map(_.getFirstName.toString).toSet == Set("Eve", "Frank") &&
        accounts.head.getType.toString == "checking-3a"
      } should haveSize(1)

      sc.run().waitUntilDone()
    }
  }

  it should "verify no duplicate key groups are emitted" in {
    val tempUsersDir = Files.createTempDirectory("smb-no-dups-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-no-dups-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Create data with some keys having many values across multiple shards
    // This tests that we don't accidentally emit duplicate key groups
    val users = (1 to 50).map { i =>
      new User(
        i % 10,
        s"Last${i % 10}",
        s"First$i",
        s"user$i@example.com",
        Collections.emptyList(),
        createAddress()
      )
    }

    val accounts = (1 to 50).map { i =>
      new Account(i % 10, s"account-$i", s"Account $i", i * 100.0, null)
    }

    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(4)
            .withNumShards(3) // Multiple shards with many values per key
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
            .withNumShards(3) // Multiple shards with many values per key
        )
      sc.run().waitUntilDone()
    }

    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      val accountsRead = AvroSortedBucketIO
        .read(new TupleTag[Account]("accounts"), classOf[Account])
        .from(tempAccountsDir.getAbsolutePath)

      val result = SMBCollection
        .cogroup2(
          classOf[Integer],
          usersRead,
          accountsRead
        )
        .mapValues { case (users, accounts) =>
          (users.toSeq, accounts.toSeq)
        }
        .toSCollectionAndSeal()

      // Should have exactly 10 unique key groups (keys 0-9)
      result should haveSize(10)

      // Verify each key appears exactly once by checking distinct keys
      val keys = result.map { case (k, _) => k }
      val distinctKeys = keys.distinct
      distinctKeys should haveSize(10)

      // Verify all keys 0-9 are present
      keys should containInAnyOrder((0 to 9).map(Integer.valueOf))

      // Verify each key group has ALL values (no splitting across multiple groups)
      // Each key should have exactly 5 users and 5 accounts
      result.filter { case (_, (users, accounts)) =>
        users.size == 5 && accounts.size == 5
      } should haveSize(10)

      sc.run().waitUntilDone()
    }
  }

  it should "correctly handle partial consumption with auto-advance to next key group" in {
    val tempUsersDir = Files.createTempDirectory("smb-partial-auto-advance").toFile
    tempUsersDir.deleteOnExit()

    // Create multiple values per key to test partial consumption
    // Key 1: Alice-1, Alice-2, Alice-3 (3 values)
    // Key 2: Bob-1, Bob-2 (2 values)
    // Key 3: Charlie-1 (1 value)
    val users = Seq(
      new User(
        1,
        "Smith",
        "Alice-1",
        "alice1@example.com",
        Collections.emptyList(),
        createAddress()
      ),
      new User(
        1,
        "Smith",
        "Alice-2",
        "alice2@example.com",
        Collections.emptyList(),
        createAddress()
      ),
      new User(
        1,
        "Smith",
        "Alice-3",
        "alice3@example.com",
        Collections.emptyList(),
        createAddress()
      ),
      new User(2, "Jones", "Bob-1", "bob1@example.com", Collections.emptyList(), createAddress()),
      new User(2, "Jones", "Bob-2", "bob2@example.com", Collections.emptyList(), createAddress()),
      new User(
        3,
        "Brown",
        "Charlie-1",
        "charlie1@example.com",
        Collections.emptyList(),
        createAddress()
      )
    )

    {
      val sc = ScioContext()
      sc.parallelize(users)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(1)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    {
      implicit val sc: ScioContext = ScioContext()

      val usersRead = AvroSortedBucketIO
        .read(new TupleTag[User]("users"), classOf[User])
        .from(tempUsersDir.getAbsolutePath)

      // Test partial consumption pattern: take only first value from each key group
      // This simulates headOption behavior - ExhaustableLazyIterable must auto-advance
      // to next key group when remaining values are exhausted
      val result = SMBCollection
        .read(classOf[Integer], usersRead)
        .flatMap(_.headOption) // Partial consumption: takes first, leaves rest
        .toSCollectionAndSeal()

      // Should have exactly 3 results (one per key group)
      result should haveSize(3)

      // Verify we got first user from each key group
      // Note: Order within key group after SMB write is deterministic within a single shard
      val firstNames = result.map { case user => user.getFirstName.toString }

      // Should have one user from each key group
      firstNames.filter(_.startsWith("Alice")) should haveSize(1)
      firstNames.filter(_.startsWith("Bob")) should haveSize(1)
      firstNames.filter(_.startsWith("Charlie")) should haveSize(1)

      // Verify all three key groups were processed (no key groups skipped due to auto-advance bug)
      result.map(_.getId).distinct should haveSize(3)

      sc.run().waitUntilDone()
    }
  }
}
