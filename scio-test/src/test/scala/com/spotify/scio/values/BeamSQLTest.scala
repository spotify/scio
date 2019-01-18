package com.spotify.scio.values

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
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

  case class UserWithFallBack(id: Long, username: String, locale: java.util.Locale)
  val usersWithLocale =
    (1 to 10).map { i =>
      UserWithFallBack(i, s"user$i", java.util.Locale.FRANCE)
    }.toList

  case class UserWithOption(username: String, email: String, age: Option[Int])
  val usersWithOption =
    (1 to 10).map { i =>
      UserWithOption(s"user$i", s"user$i@spotify.com", if (i > 5) Option(20 + i) else None)
    }.toList

  case class UserWithList(username: String, emails: List[String])
  val usersWithList =
    (1 to 10).map { i =>
      UserWithList(s"user$i", List(s"user$i@spotify.com", s"user$i@yolo.com"))
    }.toList

  val javaUsers =
    (1 to 10).map { i =>
      new com.spotify.scio.bean.UserBean(s"user$i", 20 + i)
    }
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
    val r = in.sql("select username from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support scalar results" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r = in.typedSql[Int]("select sum(age) from PCOLLECTION")
    r should containSingleValue(expected)
  }

  it should "support nested case classes" in runWithContext { sc =>
    // implicit def userIDSchema = Schema[UserId]

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
      in.sql("select id, username from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support fallback coders" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = usersWithLocale.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes = Coder.row(schemaRes)
    val in = sc.parallelize(usersWithLocale)
    val r = in.sql("select username from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "infer the schema of results" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = users.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes = Coder.row(schemaRes)
    val in = sc.parallelize(users)
    val r = in.sql("select username from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "Automatically convert rows results to Products" in runWithContext { sc =>
    val expected = users.map { u =>
      (u.username, u.age)
    }
    val in = sc.parallelize(users)
    val r = in.typedSql[(String, Int)]("select username, age from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support fallback in sql" in runWithContext { sc =>
    val expected = usersWithLocale.map { u =>
      (u.username, u.locale)
    }
    val in = sc.parallelize(usersWithLocale)
    val r = in.typedSql[(String, java.util.Locale)]("select username, locale from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support Option" in runWithContext { sc =>
    val expected = usersWithOption.map { u =>
      (u.username, u.age)
    }
    val in = sc.parallelize(usersWithOption)
    val r = in.typedSql[(String, Option[Int])]("select username, age from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support scala collections" in runWithContext { sc =>
    val expected = usersWithList.map { u =>
      (u.username, u.emails)
    }
    val in = sc.parallelize(usersWithList)
    val r = in.typedSql[(String, List[String])]("select username, emails from PCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support javabeans" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r = in.typedSql[Int]("select sum(age) from PCOLLECTION")
    r should containSingleValue(expected)
  }

  ignore should "support java collections" in runWithContext { sc =>
    // TODO
  }
}
