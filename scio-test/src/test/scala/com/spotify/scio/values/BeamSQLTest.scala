/*
 * Copyright 2016 Spotify AB.
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
package com.spotify.scio.values

import com.spotify.scio.IsJavaBean
import com.spotify.scio.coders.Coder
import com.spotify.scio.sql.Query
import com.spotify.scio.schemas.{Record, Schema}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.values.Row
import com.spotify.scio.bean.UserBean
import java.util.Locale

object TestData {
  case class Foo(i: Int, s: String)
  case class Bar(l: Long, f: Foo)
  case class Result(x: Int)

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

  case class UserWithFallBack(id: Long, username: String, locale: Locale)
  val usersWithLocale =
    (1 to 10).map { i =>
      UserWithFallBack(i, s"user$i", Locale.FRANCE)
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
      new UserBean(s"user$i", 20 + i)
    }

  case class UserWithJList(username: String, emails: java.util.List[String])
  val usersWithJList =
    (1 to 10).map { i =>
      UserWithJList(s"user$i", java.util.Arrays.asList(s"user$i@spotify.com", s"user$i@yolo.com"))
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
    val r = in.sql(Query.row("select username from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "support scalar results" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r = in.sql[Int](Query.of("select sum(age) from PCOLLECTION"))
    r should containSingleValue(expected)
  }

  it should "support nested case classes" in runWithContext { sc =>
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
    val r = in.sql(Query.row("select id, username from PCOLLECTION"))
    r should containInAnyOrder(expected)

    val in2 = sc.parallelize(usersWithIds)
    val r2 = in2.sql(Query.row("select `PCOLLECTION`.`id`.`id`, username from PCOLLECTION"))
    r2 should containInAnyOrder(expected)
  }

  it should "support fallback coders" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = usersWithLocale.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes = Coder.row(schemaRes)
    val in = sc.parallelize(usersWithLocale)
    val r = in.sql(Query.row("select username from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "infer the schema of results" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = users.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes = Coder.row(schemaRes)
    val in = sc.parallelize(users)
    val r = in.sql(Query.row("select username from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "Automatically convert rows results to Products" in runWithContext { sc =>
    val expected = users.map { u =>
      (u.username, u.age)
    }
    val in = sc.parallelize(users)
    val r = in.sql[(String, Int)](Query.of("select username, age from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "support fallback in sql" in runWithContext { sc =>
    val expected = usersWithLocale.map { u =>
      (u.username, u.locale)
    }
    val in = sc.parallelize(usersWithLocale)
    val r = in.sql[(String, Locale)](Query.of("select username, locale from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "support Option" in runWithContext { sc =>
    val expected = usersWithOption.map { u =>
      (u.username, u.age)
    }
    val in = sc.parallelize(usersWithOption)
    val r = in.sql[(String, Option[Int])](Query.of("select username, age from PCOLLECTION"))
    r should containInAnyOrder(expected)

    val in2 = sc.parallelize(usersWithOption)
    val r2 = in2.sql[Option[Int]](Query.of("select age from PCOLLECTION"))
    r2 should containInAnyOrder(expected.map(_._2))
  }

  it should "support scala collections" in runWithContext { sc =>
    val expected = usersWithList.map { u =>
      (u.username, u.emails)
    }
    val in = sc.parallelize(usersWithList)
    val r = in.sql[(String, List[String])](Query.of("select username, emails from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "support javabeans" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r = in.sql[Int](Query.of("select sum(age) from PCOLLECTION"))
    r should containSingleValue(expected)
  }

  it should "support java collections" in runWithContext { sc =>
    val expected = usersWithJList.map { u =>
      (u.username, u.emails.get(0))
    }
    val in = sc.parallelize(usersWithJList)
    val r =
      in.sql[(String, String)](Query.of("select username, emails[1] from PCOLLECTION"))
    r should containInAnyOrder(expected)
  }

  it should "not derive a Schema for non-bean Java classes" in {
    import com.spotify.scio.bean._
    "IsJavaBean[UserBean]" should compile
    "IsJavaBean[NotABean]" shouldNot compile
    "IsJavaBean[TypeMismatch]" shouldNot compile

    "Schema.javaBeanSchema[UserBean]" should compile
    "Schema.javaBeanSchema[NotABean]" shouldNot compile
    "Schema.javaBeanSchema[TypeMismatch]" shouldNot compile
  }

  // TODO: Typechecked query ?
  // TODO: Join SCollections ?
  // TODO: nested case classes query
  // TODO: convert compatible classes using nominal binding
  // TODO: Should chaining row queries be supported ?
  // TODO: Support UDF
  // ignore should "properly chain row queries" in runWithContext { sc =>
  //   val schemaRes = BSchema.builder().addInt64Field("sum(age)").build()
  //   val expected = Row.withSchema(schemaRes).addValue(255).build()

  //   val in = sc.parallelize(users)
  //   val r =
  //     in.sql(Query.row("select username, age from PCOLLECTION"))
  //       .sql(Query.row("select sum(age) from PCOLLECTION"))

  //   r should containSingleValue(expected)
  // }

  it should "properly chain typed queries" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r =
      in.sql[(String, Int)](Query.of("select username, age from PCOLLECTION"))
        .sql[Int](Query.of("select sum(_2) from PCOLLECTION"))
    r should containSingleValue(expected)
  }

  it should "Support scalar inputs" in runWithContext { sc =>
    val in = sc.parallelize((1 to 10).toList)
    val r = in.sql[Int](Query.of("select sum(`value`) from PCOLLECTION"))
    r should containSingleValue(55)
  }

  it should "support applying multiple queries on the same SCollection" in runWithContext { sc =>
    val in = sc.parallelize(users)
    val sumAges = in.sql[Int](Query.of("select sum(age) from PCOLLECTION"))
    sumAges should containSingleValue(255)
    val usernames = in.sql[String](Query.of("select username from PCOLLECTION"))
    usernames should containInAnyOrder(users.map(_.username))
  }

  it should "provide a typecheck method for tests" in {
    val Q = Query

    def checkOK[A: Schema, B: Schema](q: String) =
      Q.typecheck(Q.of[A, B](q)) should be('right)

    def checkNOK[A: Schema, B: Schema](q: String) =
      Q.typecheck(Q.of[A, B](q)) should be('left)

    checkOK[Bar, Long]("select l from PCOLLECTION")
    checkOK[Bar, Int]("select `PCOLLECTION`.`f`.`i` from PCOLLECTION")
    checkOK[Bar, Result]("select `PCOLLECTION`.`f`.`i` from PCOLLECTION")
    checkOK[Bar, Foo]("select f from PCOLLECTION")
    checkOK[Bar, (String, Long)]("select `PCOLLECTION`.`f`.`s`, l from PCOLLECTION")

    // test fallback support
    checkOK[UserWithFallBack, Locale]("select locale from PCOLLECTION")

    checkOK[UserWithOption, Option[Int]]("select age from PCOLLECTION")
    checkNOK[UserWithOption, Int]("select age from PCOLLECTION")

    checkNOK[Bar, (String, Long)]("select l from PCOLLECTION")
    checkNOK[Bar, String]("select l from PCOLLECTION")

    checkOK[Bar, Long]("""
      select cast(`PCOLLECTION`.`f`.`i` as BIGINT)
      from PCOLLECTION
    """)

    checkOK[UserBean, (String, Int)]("select name, age from PCOLLECTION")
    checkNOK[UserBean, (String, Long)]("select name, age from PCOLLECTION")
    checkNOK[UserBean, User]("select name, age from PCOLLECTION")
    checkNOK[UserBean, (String, Option[Int])]("select name, age from PCOLLECTION")
    checkNOK[UserBean, Bar]("select name, age from PCOLLECTION")
    // Calcite flattens the row value
    checkOK[UserBean, (Long, Int, String)](
      "select cast(age AS BIGINT), row(age, name) from PCOLLECTION")

    checkOK[UserBean, List[Int]]("select ARRAY[age] from PCOLLECTION")
    checkOK[UserBean, (String, List[Int])]("select name, ARRAY[age] from PCOLLECTION")
    checkNOK[UserBean, (String, Int)]("select name, ARRAY[age] from PCOLLECTION")
    checkNOK[UserBean, (String, List[Int])]("select name, age from PCOLLECTION")
  }

  it should "typecheck queries at compile time" in {
    import Query.tsql
    """tsql[Bar, Long]("select l from PCOLLECTION")""" should compile
    """tsql[Bar, Int]("select `PCOLLECTION`.`f`.`i` from PCOLLECTION")""" should compile
    """tsql[Bar, Result]("select `PCOLLECTION`.`f`.`i` from PCOLLECTION")""" should compile
    """tsql[Bar, Foo]("select f from PCOLLECTION")""" should compile
    """tsql[Bar, (String, Long)]("select `PCOLLECTION`.`f`.`s`, l from PCOLLECTION")""" should compile
    // st fallback support
    """tsql[UserWithFallBack, Locale]("select locale from PCOLLECTION")""" should compile
    """tsql[UserWithOption, Option[Int]]("select age from PCOLLECTION")""" should compile
    """tsql[Bar, Long]("select cast(`PCOLLECTION`.`f`.`i` as BIGINT) from PCOLLECTION")""" should compile
    """tsql[UserBean, (String, Int)]("select name, age from PCOLLECTION")""" should compile
    """tsql[UserBean, (Long, Int, String)]("select cast(age AS BIGINT), row(age, name) from PCOLLECTION")""" should compile
    """tsql[UserBean, List[Int]]("select ARRAY[age] from PCOLLECTION")""" should compile
    """tsql[UserBean, (String, List[Int])]("select name, ARRAY[age] from PCOLLECTION")""" should compile
    """tsql[UserWithOption, Int]("select age from PCOLLECTION")""" shouldNot compile
    """tsql[Bar, (String, Long)]("select l from PCOLLECTION")""" shouldNot compile
    """tsql[Bar, String]("select l from PCOLLECTION")""" shouldNot compile
    """tsql[UserBean, (String, Long)]("select name, age from PCOLLECTION")""" shouldNot compile
    """tsql[UserBean, User]("select name, age from PCOLLECTION")""" shouldNot compile
    """tsql[UserBean, (String, Option[Int])]("select name, age from PCOLLECTION")""" shouldNot compile
    """tsql[UserBean, Bar]("select name, age from PCOLLECTION")""" shouldNot compile
    """tsql[UserBean, (String, Int)]("select name, ARRAY[age] from PCOLLECTION")""" shouldNot compile
    """tsql[UserBean, (String, List[Int])]("select name, age from PCOLLECTION")""" shouldNot compile
  }
}
