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

import com.spotify.scio.bean.UserBean
import com.spotify.scio.schemas.To
import com.spotify.scio.values.SCollection
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.values.TupleTag

class TypedBeamSQLTest extends PipelineSpec {
  import TestData._

  // scalastyle:off line.size.limit
  "(Typed) BeamSQL" should "typecheck queries at compile time" in {
    import Queries.typed
    """typed[Bar, Long]("select l from SCOLLECTION")""" should compile
    """typed[Bar, Int]("select `SCOLLECTION`.`f`.`i` from SCOLLECTION")""" should compile
    """typed[Bar, Result]("select `SCOLLECTION`.`f`.`i` from SCOLLECTION")""" should compile
    """typed[Bar, TestData.Foo]("select f from SCOLLECTION")""" should compile
    """typed[Bar, (String, Long)]("select `SCOLLECTION`.`f`.`s`, l from SCOLLECTION")""" should compile
    // st fallback support
    // XXX: scalac :bomb: this test seems to be problematic under scala 2.11 ...
    // """tsql[UserWithFallBack, Locale]("select locale from SCOLLECTION")""" should compile
    """typed[UserWithOption, Option[Int]]("select age from SCOLLECTION")""" should compile
    """typed[Bar, Long]("select cast(`SCOLLECTION`.`f`.`i` as BIGINT) from SCOLLECTION")""" should compile
    """typed[UserBean, (String, Int)]("select name, age from SCOLLECTION")""" should compile
    """typed[UserBean, (Long, Int, String)]("select cast(age AS BIGINT), row(age, name) from SCOLLECTION")""" should compile
    """typed[UserBean, List[Int]]("select ARRAY[age] from SCOLLECTION")""" should compile
    """typed[UserBean, (String, List[Int])]("select name, ARRAY[age] from SCOLLECTION")""" should compile
    """typed[UserWithOption, Int]("select age from SCOLLECTION")""" shouldNot compile
    """typed[Bar, (String, Long)]("select l from SCOLLECTION")""" shouldNot compile
    """typed[Bar, String]("select l from SCOLLECTION")""" shouldNot compile
    """typed[UserBean, (String, Long)]("select name, age from SCOLLECTION")""" shouldNot compile
    """typed[UserBean, User]("select name, age from SCOLLECTION")""" shouldNot compile
    """typed[UserBean, (String, Option[Int])]("select name, age from SCOLLECTION")""" shouldNot compile
    """typed[UserBean, Bar]("select name, age from SCOLLECTION")""" shouldNot compile
    """typed[UserBean, (String, Int)]("select name, ARRAY[age] from SCOLLECTION")""" shouldNot compile
    """typed[UserBean, (String, List[Int])]("select name, age from SCOLLECTION")""" shouldNot compile
  }

  it should "typecheck queries with JOINs" in {
    import Queries.typed
    """
    |typed[User, User, String]("select a.username from B a join A b on a.username = b.username", new TupleTag[User]("A"), new TupleTag[User]("B"))
    |""".stripMargin should compile
    """
    |typed[User, User, Int]("select a.username from B a join A b on a.username = b.username", new TupleTag[User]("A"), new TupleTag[User]("B"))
    |""".stripMargin shouldNot compile
    """
    |typed[User, User, String]("select a.username from B a join A b on a.username = b.username", new TupleTag[User]("C"), new TupleTag[User]("D"))
    |""".stripMargin shouldNot compile
  }
  // scalastyle:on line.size.limit

  it should "give a clear error message when the query can not be checked at compile time" in {
    """
    val q = "select name, age from SCOLLECTION"
    Queries.typed[UserBean, (String, Int)](q)
    """ shouldNot compile

    """
    def functionName(q: String) = Queries.typed[(String, String), String](q)
    """ shouldNot compile
  }

  it should "typecheck classes compatibilty" in {
    import TypeConvertionsTestData._
    """To.safe[TinyTo, From0]""" shouldNot compile
    """To.safe[From0, CompatibleAvroTestRecord]""" shouldNot compile
  }

  "String interpolation" should "statically check interpolated queries" in runWithContext { sc =>
    """
    def coll: SCollection[(Int, String)] =
      sc.parallelize((1 to 10).toList.map(i => (i, i.toString)))
    val r: SCollection[Int] = tsql"SELECT _1 FROM $coll"
    """ should compile

    """
    def coll: SCollection[(Int, String)] =
      sc.parallelize((1 to 10).toList.map(i => (i, i.toString)))
    val r: SCollection[String] = tsql"SELECT _1 FROM $coll"
    """ shouldNot compile
  }

}
