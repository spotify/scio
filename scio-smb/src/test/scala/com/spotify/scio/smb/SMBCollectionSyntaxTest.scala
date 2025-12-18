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
import com.spotify.scio.smb.syntax.all._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag

import java.nio.file.Files
import java.util.Collections

/**
 * Tests for SMBCollection join syntax helpers.
 *
 * Tests smbJoin, smbLeftJoin, smbRightJoin, and smbOuterJoin.
 */
class SMBCollectionSyntaxTest extends PipelineSpec {

  private val address = new Address("street1", "street2", "city", "state", "01234", "USA")

  private val user1 =
    new User(1, "Smith", "Alice", "alice@example.com", Collections.emptyList(), address)
  private val user2 =
    new User(2, "Jones", "Bob", "bob@example.com", Collections.emptyList(), address)
  private val user3 =
    new User(3, "Brown", "Charlie", "charlie@example.com", Collections.emptyList(), address)

  private val account1 = new Account(1, "savings", "Alice Savings", 1000.0, null)
  private val account2 = new Account(2, "checking", "Bob Checking", 500.0, null)
  private val account4 = new Account(4, "investment", "Dave Investment", 2000.0, null)

  private def setupTestData(): (String, String) = {
    val tempUsersDir = Files.createTempDirectory("smb-syntax-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-syntax-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Write users (IDs: 1, 2, 3)
    {
      val sc = ScioContext()
      sc.parallelize(Seq(user1, user2, user3))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[User])
            .to(tempUsersDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    // Write accounts (IDs: 1, 2, 4)
    {
      val sc = ScioContext()
      sc.parallelize(Seq(account1, account2, account4))
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(tempAccountsDir.getAbsolutePath)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run().waitUntilDone()
    }

    (tempUsersDir.getAbsolutePath, tempAccountsDir.getAbsolutePath)
  }

  "smbJoin" should "perform inner join returning only matching keys" in {
    val (usersPath, accountsPath) = setupTestData()

    implicit val sc: ScioContext = ScioContext()

    val usersRead = AvroSortedBucketIO
      .read(new TupleTag[User]("users"), classOf[User])
      .from(usersPath)

    val accountsRead = AvroSortedBucketIO
      .read(new TupleTag[Account]("accounts"), classOf[Account])
      .from(accountsPath)

    val result = smbJoin(classOf[Integer], usersRead, accountsRead)
      .toSCollectionAndSeal()

    // Inner join: only keys 1 and 2 have matches on both sides
    result should haveSize(2)

    // Verify the joined data
    result.filter { case (k, (user, account)) =>
      k == 1 && user.getId == 1 && account.getId == 1
    } should haveSize(1)

    result.filter { case (k, (user, account)) =>
      k == 2 && user.getId == 2 && account.getId == 2
    } should haveSize(1)

    sc.run().waitUntilDone()
  }

  "smbLeftJoin" should "include all left records with optional right matches" in {
    val (usersPath, accountsPath) = setupTestData()

    implicit val sc: ScioContext = ScioContext()

    val usersRead = AvroSortedBucketIO
      .read(new TupleTag[User]("users"), classOf[User])
      .from(usersPath)

    val accountsRead = AvroSortedBucketIO
      .read(new TupleTag[Account]("accounts"), classOf[Account])
      .from(accountsPath)

    val result = smbLeftJoin(classOf[Integer], usersRead, accountsRead)
      .mapValues { case (user, accountOpt) =>
        (user.getId, accountOpt.map(_.getId))
      }
      .toSCollectionAndSeal()

    // Left join: all users (1, 2, 3)
    result should haveSize(3)

    // User 1 has account
    result.filter { case (k, (userId, accountIdOpt)) =>
      k == 1 && userId == 1 && accountIdOpt.contains(1)
    } should haveSize(1)

    // User 2 has account
    result.filter { case (k, (userId, accountIdOpt)) =>
      k == 2 && userId == 2 && accountIdOpt.contains(2)
    } should haveSize(1)

    // User 3 has no account (None)
    result.filter { case (k, (userId, accountIdOpt)) =>
      k == 3 && userId == 3 && accountIdOpt.isEmpty
    } should haveSize(1)

    sc.run().waitUntilDone()
  }

  "smbRightJoin" should "include all right records with optional left matches" in {
    val (usersPath, accountsPath) = setupTestData()

    implicit val sc: ScioContext = ScioContext()

    val usersRead = AvroSortedBucketIO
      .read(new TupleTag[User]("users"), classOf[User])
      .from(usersPath)

    val accountsRead = AvroSortedBucketIO
      .read(new TupleTag[Account]("accounts"), classOf[Account])
      .from(accountsPath)

    val result = smbRightJoin(classOf[Integer], usersRead, accountsRead)
      .mapValues { case (userOpt, account) =>
        (userOpt.map(_.getId), account.getId)
      }
      .toSCollectionAndSeal()

    // Right join: all accounts (1, 2, 4)
    result should haveSize(3)

    // Account 1 has user
    result.filter { case (k, (userIdOpt, accountId)) =>
      k == 1 && userIdOpt.contains(1) && accountId == 1
    } should haveSize(1)

    // Account 2 has user
    result.filter { case (k, (userIdOpt, accountId)) =>
      k == 2 && userIdOpt.contains(2) && accountId == 2
    } should haveSize(1)

    // Account 4 has no user (None)
    result.filter { case (k, (userIdOpt, accountId)) =>
      k == 4 && userIdOpt.isEmpty && accountId == 4
    } should haveSize(1)

    sc.run().waitUntilDone()
  }

  "smbOuterJoin" should "include all records from both sides with optional matches" in {
    val (usersPath, accountsPath) = setupTestData()

    implicit val sc: ScioContext = ScioContext()

    val usersRead = AvroSortedBucketIO
      .read(new TupleTag[User]("users"), classOf[User])
      .from(usersPath)

    val accountsRead = AvroSortedBucketIO
      .read(new TupleTag[Account]("accounts"), classOf[Account])
      .from(accountsPath)

    val result = smbOuterJoin(classOf[Integer], usersRead, accountsRead)
      .mapValues { case (userOpt, accountOpt) =>
        (userOpt.map(_.getId), accountOpt.map(_.getId))
      }
      .toSCollectionAndSeal()

    // Outer join: keys 1, 2, 3, 4
    result should haveSize(4)

    // Key 1: both sides
    result.filter { case (k, (userIdOpt, accountIdOpt)) =>
      k == 1 && userIdOpt.contains(1) && accountIdOpt.contains(1)
    } should haveSize(1)

    // Key 2: both sides
    result.filter { case (k, (userIdOpt, accountIdOpt)) =>
      k == 2 && userIdOpt.contains(2) && accountIdOpt.contains(2)
    } should haveSize(1)

    // Key 3: only user (account is None)
    result.filter { case (k, (userIdOpt, accountIdOpt)) =>
      k == 3 && userIdOpt.contains(3) && accountIdOpt.isEmpty
    } should haveSize(1)

    // Key 4: only account (user is None)
    result.filter { case (k, (userIdOpt, accountIdOpt)) =>
      k == 4 && userIdOpt.isEmpty && accountIdOpt.contains(4)
    } should haveSize(1)

    sc.run().waitUntilDone()
  }

  "smbJoin" should "handle empty inputs" in {
    val tempUsersDir = Files.createTempDirectory("smb-syntax-empty-users").toFile
    val tempAccountsDir = Files.createTempDirectory("smb-syntax-empty-accounts").toFile

    tempUsersDir.deleteOnExit()
    tempAccountsDir.deleteOnExit()

    // Write empty users
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

    // Write empty accounts
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

    implicit val sc: ScioContext = ScioContext()

    val usersRead = AvroSortedBucketIO
      .read(new TupleTag[User]("users"), classOf[User])
      .from(tempUsersDir.getAbsolutePath)

    val accountsRead = AvroSortedBucketIO
      .read(new TupleTag[Account]("accounts"), classOf[Account])
      .from(tempAccountsDir.getAbsolutePath)

    val result = smbJoin(classOf[Integer], usersRead, accountsRead)
      .toSCollectionAndSeal()

    result should beEmpty

    sc.run().waitUntilDone()
  }
}
