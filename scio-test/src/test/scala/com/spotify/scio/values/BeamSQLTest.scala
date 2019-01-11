package com.spotify.scio.values

import com.spotify.scio.coders.{Coder, Schema}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.values.Row

object TestData {
  case class User(username: String, email: String, age: Int)
  val users =
    (1 to 10).map { i =>
      User(s"user$i", s"user$i@spotify.com", 20 + i)
    }.toList

  case class UserId(id: Long)
  case class UserWithId(id: UserId, username: String, email: String, age: Int)

  val usersWithIds =
    (1 to 10).map { i =>
      UserWithId(UserId(i), s"user$i", s"user$i@spotify.com", 20 + i)
    }.toList
}

class BeamSQLTest extends PipelineSpec {
  import TestData._

  "BeamSQL" should "support queries on case classes" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = users.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes = Coder.row(schemaRes)
    val in = sc.parallelize(users)
    val r = in.applyTransform(SqlTransform.query("select username from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "support nested case classes" in runWithContext { sc =>
    implicit def userIDSchema = Schema[UserId]

    val schemaRes =
      BSchema
        .builder()
        .addInt64Field("id")
        .addStringField("username")
        .build()

    val expected = usersWithIds.map { u =>
      Row
        .withSchema(schemaRes)
        .addValue(u.id.id)
        .addValue(u.username)
        .build()
    }

    implicit def coderRowRes = Coder.row(schemaRes)
    val in = sc.parallelize(usersWithIds)
    val r =
      in.applyTransform(SqlTransform.query("select id, username from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  ignore should "support fallback coders" in {
    // TODO
  }

  ignore should "infer the schema of results" in {
    // TODO
  }

  ignore should "Automatically convert rows results to Products" in {
    // TODO
  }
}
