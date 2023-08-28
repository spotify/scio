package com.spotify.scio.smb

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.avro.{Account, Address, User}
import com.spotify.scio.io.{CustomIO, TextIO}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, TargetParallelism}
import org.apache.beam.sdk.values.TupleTag

import java.util.Collections

object SmbJob {

  def main(cmdlineArgs: Array[String]) = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.sortMergeJoin(
      classOf[Integer],
      AvroSortedBucketIO
        .read(new TupleTag[User]("lhs"), classOf[User])
        .from(args("users")),
      AvroSortedBucketIO
        .read(new TupleTag[Account]("rhs"), classOf[Account])
        .from(args("accounts")),
      TargetParallelism.max()
    ).values
      .map { case (u, a) => s"${u.getLastName}=${a.getAmount}" }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
  }

}

class SortMergeBucketTest extends PipelineSpec {

  "SMB" should "be able to mock input and output" in {
    val account: Account = new Account(1, "type", "name", 12.5, null)
    val address = new Address("street1", "street2", "city", "state", "01234", "Sweden")
    val user =
      new User(1, "lastname", "firstname", "email@foobar.com", Collections.emptyList(), address)

    JobTest[SmbJob.type]
      .args(
        "--users=users",
        "--accounts=accounts",
        "--output=output"
      )
      .keyedInput(SMBIO[Integer, User]("users", _.getId), Seq(user))
      // input is also possible but error prone as key must be given manually
      .input(SMBIO[Integer, Account]("accounts", _.getId), Seq(account.getId -> account))
      .output(TextIO("output"))(_ should containInAnyOrder(Seq("lastname=12.5")))
      .run()
  }

}
