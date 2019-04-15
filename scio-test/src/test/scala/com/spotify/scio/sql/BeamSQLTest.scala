/*
 * Copyright 2019 Spotify AB.
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
package com.spotify.scio.sql

import java.util.Locale
import java.lang.{Iterable => JIterable}

import com.spotify.scio.IsJavaBean
import com.spotify.scio.bean.UserBean
import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas.{Schema, To}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{Row, TupleTag}
import org.scalatest.Assertion

import scala.collection.JavaConverters._
import com.spotify.scio.avro

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

  case class UserWithMap(username: String, contacts: Map[String, String])
  val usersWithMap =
    (1 to 10).map { i =>
      UserWithMap(s"user$i", Map("work" -> s"user$i@spotify.com", "personal" -> s"user$i@yolo.com"))
    }.toList

  class IsOver18UdfFn extends SerializableFunction[Integer, Boolean] {
    override def apply(input: Integer): Boolean = input >= 18
  }

  class IsOver18Udf extends BeamSqlUdf {
    def eval(input: Integer): Boolean = input >= 18
  }

  class MaxUserAgeUdafFn extends CombineFn[Integer, Integer, Integer] {
    override def createAccumulator(): Integer = 0

    override def addInput(accumulator: Integer, input: Integer): Integer =
      Math.max(accumulator, input)

    override def mergeAccumulators(accumulators: JIterable[Integer]): Integer =
      accumulators.asScala.max

    override def extractOutput(accumulator: Integer): Integer = accumulator
  }

  val avroUsers: List[avro.User] =
    (1 to 10).map { i =>
      val addr =
        new avro.Address(
          s"street1_$i",
          s"street2_$i",
          s"city_$i",
          s"",
          s"114 36",
          s"SE"
        )
      new avro.User(i, s"lastname_$i", s"firstname_$i", s"email$i@spotify.com", Nil.asJava, addr)
    }.toList
}

class BeamSQLTest extends PipelineSpec {
  import TestData._

  "BeamSQL" should "support queries on case classes" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = users.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes: Coder[Row] = Coder.row(schemaRes)
    val in = sc.parallelize(users)
    val r = in.query("select username from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support different tag" in runWithContext { sc =>
    val expected = 255
    val q = Query[User, Int]("select sum(age) from users", tag = new TupleTag[User]("users"))
    val r = sc.parallelize(users).queryAs(q)
    r should containSingleValue(expected)
  }

  it should "support scalar results" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r = in.queryAs[Int]("select sum(age) from SCOLLECTION")
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

    implicit def coderRowRes: Coder[Row] = Coder.row(schemaRes)
    val in = sc.parallelize(usersWithIds)
    val r = in.query("select id, username from SCOLLECTION")
    r should containInAnyOrder(expected)

    val in2 = sc.parallelize(usersWithIds)
    val r2 = in2.query("select `SCOLLECTION`.`id`.`id`, username from SCOLLECTION")
    r2 should containInAnyOrder(expected)
  }

  it should "support fallback coders" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = usersWithLocale.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes: Coder[Row] = Coder.row(schemaRes)
    val in = sc.parallelize(usersWithLocale)
    val r = in.query("select username from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "infer the schema of results" in runWithContext { sc =>
    val schemaRes = BSchema.builder().addStringField("username").build()
    val expected = users.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).build()
    }
    implicit def coderRowRes: Coder[Row] = Coder.row(schemaRes)
    val in = sc.parallelize(users)
    val r = in.query("select username from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "Automatically convert rows results to Products" in runWithContext { sc =>
    val expected = users.map { u =>
      (u.username, u.age)
    }
    val in = sc.parallelize(users)
    val r = in.queryAs[(String, Int)]("select username, age from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support fallback in sql" in runWithContext { sc =>
    val expected = usersWithLocale.map { u =>
      (u.username, u.locale)
    }
    val in = sc.parallelize(usersWithLocale)
    val r = in.queryAs[(String, Locale)]("select username, locale from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support Option" in runWithContext { sc =>
    val expected = usersWithOption.map { u =>
      (u.username, u.age)
    }
    val in = sc.parallelize(usersWithOption)
    val r = in.queryAs[(String, Option[Int])]("select username, age from SCOLLECTION")
    r should containInAnyOrder(expected)

    val in2 = sc.parallelize(usersWithOption)
    val r2 = in2.queryAs[Option[Int]]("select age from SCOLLECTION")
    r2 should containInAnyOrder(expected.map(_._2))
  }

  it should "support scala collections" in runWithContext { sc =>
    val expected = usersWithList.map { u =>
      (u.username, u.emails)
    }
    val in = sc.parallelize(usersWithList)
    val r = in.queryAs[(String, List[String])]("select username, emails from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support javabeans" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r = in.queryAs[Int]("select sum(age) from SCOLLECTION")
    r should containSingleValue(expected)
  }

  it should "support java collections" in runWithContext { sc =>
    val expected = usersWithJList.map { u =>
      (u.username, u.emails.get(0))
    }
    val in = sc.parallelize(usersWithJList)
    val r =
      in.queryAs[(String, String)]("select username, emails[1] from SCOLLECTION")
    r should containInAnyOrder(expected)
  }

  it should "support Map[K, V]" in runWithContext { sc =>
    val expected = usersWithMap.map { u =>
      (u.username, u.contacts)
    }
    val in = sc.parallelize(usersWithMap)
    val r =
      in.queryAs[(String, Map[String, String])]("select username, contacts from SCOLLECTION")
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

  it should "support JOIN" in runWithContext { sc =>
    val a = sc.parallelize(users)
    val b = sc.parallelize(users)

    Sql
      .from(a, b)
      .query("select a.username from B a join A b on a.username = b.username",
             new TupleTag[User]("A"),
             new TupleTag[User]("B")) shouldNot beEmpty

    Sql
      .from(a, b)
      .queryAs[String]("select a.username from B a join A b on a.username = b.username",
                       new TupleTag[User]("A"),
                       new TupleTag[User]("B")) shouldNot beEmpty
  }

  it should "properly chain typed queries" in runWithContext { sc =>
    val expected = 255
    val in = sc.parallelize(users)
    val r1 =
      in.queryAs[(String, Int)]("select username, age from SCOLLECTION")
        .queryAs[Int]("select sum(_2) from SCOLLECTION")
    r1 should containSingleValue(expected)

    val inB = sc.parallelize(users)
    val r2 = Sql
      .from(in, inB)
      .queryAs[(String, Int)](
        "select a.username, b.age from B a join A b on a.username = b.username",
        new TupleTag[User]("A"),
        new TupleTag[User]("B"))
      .queryAs[Int]("select sum(_2) from SCOLLECTION")

    r2 should containSingleValue(expected)
  }

  it should "Support scalar inputs" in runWithContext { sc =>
    val in = sc.parallelize((1 to 10).toList)
    val r = in.queryAs[Int]("select sum(`value`) from SCOLLECTION")
    r should containSingleValue(55)
  }

  it should "support applying multiple queries on the same SCollection" in runWithContext { sc =>
    val in = sc.parallelize(users)
    val sumAges = in.queryAs[Int]("select sum(age) from SCOLLECTION")
    sumAges should containSingleValue(255)
    val usernames = in.queryAs[String]("select username from SCOLLECTION")
    usernames should containInAnyOrder(users.map(_.username))
  }

  it should "provide a typecheck method for tests" in {
    object checkOK {
      def apply[A: Schema, B: Schema](q: String): Assertion =
        Queries.typecheck(Query[A, B](q)) should be('right)

      def apply[A: Schema, B: Schema, C: Schema](q: String,
                                                 a: TupleTag[A],
                                                 b: TupleTag[B]): Assertion =
        Queries.typecheck(Query2[A, B, C](q, a, b)) should be('right)
    }

    object checkNOK {
      def apply[A: Schema, B: Schema](q: String): Assertion =
        Queries.typecheck(Query[A, B](q)) should be('left)

      def apply[A: Schema, B: Schema, C: Schema](q: String,
                                                 a: TupleTag[A],
                                                 b: TupleTag[B]): Assertion =
        Queries.typecheck(Query2[A, B, C](q, a, b)) should be('left)
    }

    checkOK[Bar, Long]("select l from SCOLLECTION")
    checkOK[Bar, Int]("select `SCOLLECTION`.`f`.`i` from SCOLLECTION")
    checkOK[Bar, Result]("select `SCOLLECTION`.`f`.`i` from SCOLLECTION")
    checkOK[Bar, Foo]("select f from SCOLLECTION")
    checkOK[Bar, (String, Long)]("select `SCOLLECTION`.`f`.`s`, l from SCOLLECTION")

    // test fallback support
    checkOK[UserWithFallBack, Locale]("select locale from SCOLLECTION")

    checkOK[UserWithOption, Option[Int]]("select age from SCOLLECTION")
    checkNOK[UserWithOption, Int]("select age from SCOLLECTION")

    checkNOK[Bar, (String, Long)]("select l from SCOLLECTION")
    checkNOK[Bar, String]("select l from SCOLLECTION")

    checkOK[Bar, Long]("""
      select cast(`SCOLLECTION`.`f`.`i` as BIGINT)
      from SCOLLECTION
    """)

    checkOK[UserBean, (String, Int)]("select name, age from SCOLLECTION")
    checkNOK[UserBean, (String, Long)]("select name, age from SCOLLECTION")
    checkNOK[UserBean, User]("select name, age from SCOLLECTION")
    checkNOK[UserBean, (String, Option[Int])]("select name, age from SCOLLECTION")
    checkNOK[UserBean, Bar]("select name, age from SCOLLECTION")
    // Calcite flattens the row value
    checkOK[UserBean, (Long, Int, String)](
      "select cast(age AS BIGINT), row(age, name) from SCOLLECTION")

    checkOK[UserBean, List[Int]]("select ARRAY[age] from SCOLLECTION")
    checkOK[UserBean, (String, List[Int])]("select name, ARRAY[age] from SCOLLECTION")
    checkNOK[UserBean, (String, Int)]("select name, ARRAY[age] from SCOLLECTION")
    checkNOK[UserBean, (String, List[Int])]("select name, age from SCOLLECTION")

    checkOK[User, User, (String, Int)](
      "select a.username, b.age from A a join B b on a.username = b.username",
      new TupleTag[User]("A"),
      new TupleTag[User]("B"))
    checkNOK[User, User, (String, String)](
      "select a.username, b.age from A a join B b on a.username = b.username",
      new TupleTag[User]("A"),
      new TupleTag[User]("B"))
  }

  it should "support UDFs from SerializableFunctions and classes" in runWithContext { sc =>
    val schemaRes = BSchema
      .builder()
      .addStringField("username")
      .addBooleanField("isOver18")
      .build()

    val expected = users.map { u =>
      Row.withSchema(schemaRes).addValue(u.username).addValue(true).build()
    }
    implicit def coderRowRes: Coder[Row] = Coder.row(schemaRes)

    val in = sc.parallelize(users)

    in.query(
      "select username, isUserOver18(age) as isOver18 from SCOLLECTION",
      Udf.fromSerializableFn("isUserOver18", new IsOver18UdfFn())
    ) should containInAnyOrder(expected)

    in.query(
      "select username, isUserOver18(age) as isOver18 from SCOLLECTION",
      Udf.fromClass("isUserOver18", classOf[IsOver18Udf])
    ) should containInAnyOrder(expected)
  }

  it should "support UDAFs from CombineFns" in runWithContext { sc =>
    val schemaRes = BSchema
      .builder()
      .addInt32Field("maxUserAge")
      .build()

    val expected = Seq(Row.withSchema(schemaRes).addValue(30).build())
    implicit def coderRowRes: Coder[Row] = Coder.row(schemaRes)

    sc.parallelize(users)
      .query(
        "select maxUserAge(age) as maxUserAge from SCOLLECTION",
        Udf.fromAggregateFn("maxUserAge", new MaxUserAgeUdafFn())
      ) should containInAnyOrder(expected)
  }

  it should "automatically convert from compatible classes" in runWithContext { sc =>
    import TypeConvertionsTestData._
    sc.parallelize(from)
      .to[To1](To.unsafe) should containInAnyOrder(to)

    sc.parallelize(javaUsers)
      .to[JavaCompatibleUser](To.unsafe) should containInAnyOrder(expectedJavaCompatUsers)

    sc.parallelize(from)
      .to[TinyTo](To.unsafe) should containInAnyOrder(tinyTo)

    sc.parallelize(from)
      .to[To1](To.safe) should containInAnyOrder(to)

    sc.parallelize(javaUsers)
      .to[JavaCompatibleUser](To.safe) should containInAnyOrder(expectedJavaCompatUsers)

    sc.parallelize(from)
      .to[TinyTo](To.safe) should containInAnyOrder(tinyTo)
  }

  it should "Support queries on Avro generated classes" in runWithContext { sc =>
    val expected: List[(Int, String, String)] =
      avroUsers.map { u =>
        (u.getId.toInt, u.getFirstName.toString, u.getLastName.toString)
      }

    val q =
      Query[avro.User, (Int, String, String)]("SELECT id, first_name, last_name from SCOLLECTION")

    sc.parallelize(avroUsers).queryAs(q) should containInAnyOrder(expected)
  }

  it should "Automatically convert from Avro to Scala" in runWithContext { sc =>
    import TypeConvertionsTestData._
    val expected: List[AvroCompatibleUser] =
      avroUsers.map { u =>
        AvroCompatibleUser(u.getId.toInt, u.getFirstName.toString, u.getLastName.toString)
      }

    sc.parallelize(avroUsers)
      .to[AvroCompatibleUser](To.unsafe) should containInAnyOrder(expected)

    // Test support for nullable fields
    sc.parallelize(avroWithNullable)
      .to[CompatibleAvroTestRecord](To.unsafe) should containInAnyOrder(expectedAvro)

    sc.parallelize(avroUsers)
      .to[AvroCompatibleUser](To.safe) should containInAnyOrder(expected)

    sc.parallelize(avroWithNullable)
      .to[CompatibleAvroTestRecord](To.safe) should containInAnyOrder(expectedAvro)
  }
}

object TypeConvertionsTestData {
  import TestData._
  case class From1(xs: List[Long], q: String)
  case class From0(i: Int, s: String, e: From1)

  case class To0(q: String, xs: List[Long])
  case class To1(s: String, e: To0, i: Int)

  case class TinyTo(s: String, i: Int)

  val from =
    (1 to 10).map { i =>
      From0(1, s"from0_$i", From1((20 to 30).toList.map(_.toLong), s"from1_$i"))
    }.toList

  val to =
    from.map {
      case From0(i, s, From1(xs, q)) =>
        To1(s, To0(q, xs), i)
    }.toList

  val tinyTo = to.map {
    case To1(s, _, i) => TinyTo(s, i)
  }

  case class JavaCompatibleUser(name: String, age: Int)
  val expectedJavaCompatUsers =
    javaUsers.map { j =>
      JavaCompatibleUser(j.getName, j.getAge)
    }

  case class AvroCompatibleUser(id: Int, first_name: String, last_name: String)

  val avroWithNullable =
    (1 to 10).map { i =>
      new avro.TestRecord(i, i, null, null, false, null, List[CharSequence](s"value_$i").asJava)
    }.toList

  val expectedAvro =
    avroWithNullable.map { r =>
      CompatibleAvroTestRecord(
        Option(r.getIntField.toInt),
        Option(r.getLongField.toLong),
        Option(r.getStringField).map(_.toString),
        r.getArrayField.asScala.toList.map(_.toString)
      )
    }.toList

  case class CompatibleAvroTestRecord(
    int_field: Option[Int],
    long_field: Option[Long],
    string_field: Option[String],
    array_field: List[String]
  )
}
