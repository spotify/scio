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

package com.spotify.scio.smb

import com.spotify.scio.avro.{Account, AccountStatus, Address, User}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.{Args, ContextAndArgs, ScioContext}
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.{KV, TupleTag}

import java.nio.file.Files
import java.util.Collections
import scala.jdk.CollectionConverters._

trait SmbJob {

  val keyClass: Class[Integer] = classOf
  val keyField: String = "id"

  def avroUsers(args: Args): AvroSortedBucketIO.Read[User] = AvroSortedBucketIO
    .read(new TupleTag[User]("user"), classOf[User])
    .from(args("users"))

  def avroAccounts(args: Args): AvroSortedBucketIO.Read[Account] = AvroSortedBucketIO
    .read(new TupleTag[Account]("account"), classOf[Account])
    .from(args("accounts"))

  def avroOutput(args: Args): AvroSortedBucketIO.Write[Integer, Void, User] = AvroSortedBucketIO
    .write(keyClass, keyField, classOf[User])
    .to(args("output"))

  def avroTransformOutput(args: Args): AvroSortedBucketIO.TransformOutput[Integer, Void, User] =
    AvroSortedBucketIO
      .transformOutput(keyClass, keyField, classOf[User])
      .to(args("output"))

  def setUserAccounts(users: Iterable[User], accounts: Iterable[Account]): User = {
    val u :: Nil = users.toList
    setUserAccounts(u, accounts)
  }

  def setUserAccounts(user: User, accounts: Iterable[Account]): User = {
    val sortedAccounts = accounts.toList
      .sortBy(_.getAmount)(Ordering[java.lang.Double].reverse)
      .asJava
    User
      .newBuilder(user)
      .setAccounts(sortedAccounts)
      .build()
  }

}

object SmbJoinSaveJob extends SmbJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.sortMergeJoin(
      keyClass,
      avroUsers(args),
      avroAccounts(args)
    ).values
      .map { case (u, a) => setUserAccounts(u, Iterable(a)) }
      .saveAsSortedBucket(avroOutput(args))

    sc.run().waitUntilDone()
  }

}

object SmbCoGroupSavePreKeyedJob extends SmbJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.sortMergeCoGroup(
      keyClass,
      avroUsers(args),
      avroAccounts(args)
    ).map { case (id, (us, as)) => KV.of(id, setUserAccounts(us, as)) }
      .saveAsPreKeyedSortedBucket(avroOutput(args))

    sc.run().waitUntilDone()
  }

}

object SmbTransformJob extends SmbJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.sortMergeTransform(
      keyClass,
      avroUsers(args),
      avroAccounts(args)
    ).to(avroTransformOutput(args))
      .via { case (_, (users, accounts), outputCollector) =>
        outputCollector.accept(setUserAccounts(users, accounts))
      }

    sc.run().waitUntilDone()
  }

}

class SmbIOTest extends PipelineSpec {
  private val accountA =
    new Account(1, "typeA", "nameA", 12.5, null)
  private val accountB =
    new Account(1, "typeB", "nameB", 7.0, null)
  private val address =
    new Address("street1", "street2", "city", "state", "01234", "Sweden")
  private val user =
    new User(1, "lastname", "firstname", "email@foobar.com", Collections.emptyList(), address)
  private val joinedUserAccounts =
    User.newBuilder(user).setAccounts(List(accountA, accountB).asJava).build()

  "SmbIO" should "be able to mock sortMergeTransform input and saveAsSortedBucket output" in {
    JobTest[SmbJoinSaveJob.type]
      .args(
        "--users=gs://users",
        "--accounts=gs://accounts",
        "--output=gs://output"
      )
      .input(SmbIO[Integer, User]("gs://users", _.getId), Seq(user))
      .input(SmbIO[Integer, Account]("gs://accounts", _.getId), Seq(accountA, accountB))
      .output(SmbIO[Integer, User]("gs://output", _.getId))(
        _ should containInAnyOrder(
          Seq(
            User.newBuilder(user).setAccounts(Collections.singletonList(accountA)).build(),
            User.newBuilder(user).setAccounts(Collections.singletonList(accountB)).build()
          )
        )
      )
      .run()
  }

  it should "be able to mock sortMergeCoGroup and saveAsSortedBucket" in {
    JobTest[SmbCoGroupSavePreKeyedJob.type]
      .args(
        "--users=gs://users",
        "--accounts=gs://accounts",
        "--output=gs://output"
      )
      .input(SmbIO[Integer, User]("gs://users", _.getId), Seq(user))
      .input(SmbIO[Integer, Account]("gs://accounts", _.getId), Seq(accountA, accountB))
      .output(SmbIO[Integer, User]("gs://output", _.getId))(
        _ should containInAnyOrder(Seq(joinedUserAccounts))
      )
      .run()
  }

  it should "be able to mock sortMergeTransform" in {
    JobTest[SmbTransformJob.type]
      .args(
        "--users=gs://users",
        "--accounts=gs://accounts",
        "--output=gs://output"
      )
      .input(SmbIO[Integer, User]("gs://users", _.getId), Seq(user))
      .input(SmbIO[Integer, Account]("gs://accounts", _.getId), Seq(accountA, accountB))
      .output(SmbIO[Integer, User]("gs://output", _.getId))(
        _ should containInAnyOrder(Seq(joinedUserAccounts))
      )
      .run()
  }

  "SortedBucketTap" should "work SMB writes" in {
    val accounts = (1 to 100)
      .map { i =>
        Account
          .newBuilder()
          .setId(i)
          .setName(i.toString)
          .setType(i.toString)
          .setAccountStatus(AccountStatus.Active)
          .setAmount(i.toDouble)
          .build()
      }

    val tempFolder = Files.createTempDirectory("smb-tap")
    val sc = ScioContext()
    val tap = sc
      .parallelize(accounts)
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(classOf[String], "name", classOf[Account])
          .to(tempFolder.toFile.getAbsolutePath)
          .withNumBuckets(4)
          .withNumShards(2)
          .withFilenamePrefix("custom-prefix")
      )

    val result = sc.run().waitUntilDone()
    tap.get(result).value.toSeq should contain theSameElementsAs accounts
  }
}
