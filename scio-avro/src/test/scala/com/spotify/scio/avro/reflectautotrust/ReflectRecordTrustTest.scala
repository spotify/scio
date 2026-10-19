/*
 * Copyright 2026 Spotify AB
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

// no test trusts these packages, so ReflectRecordDatumFactory's own trust is what's tested
package com.spotify.scio.avro.reflectautotrust {

  import com.spotify.scio.avro._
  import com.spotify.scio.avro.reflectautotrust.other.{Inner, Item, Unrelated}
  import com.spotify.scio.coders.Coder
  import com.spotify.scio.testing.CoderAssertions._
  import org.scalatest.flatspec.AnyFlatSpec
  import org.scalatest.matchers.should.Matchers

  import java.math.BigDecimal
  import scala.jdk.CollectionConverters._

  class ReflectRecordTrustTest extends AnyFlatSpec with Matchers {

    // ClassSecurityValidator only exists in avro 1.12.1+; older avro trusts every class
    // looked up on each call: AvroClassTrust.install replaces the global validator
    private def validator: Option[(AnyRef, java.lang.reflect.Method)] =
      scala.util
        .Try(Class.forName("org.apache.avro.util.ClassSecurityValidator"))
        .toOption
        .map { cls =>
          val predicate =
            Class.forName("org.apache.avro.util.ClassSecurityValidator$ClassSecurityPredicate")
          (
            cls.getMethod("getGlobal").invoke(null),
            predicate.getMethod("isTrusted", classOf[Class[_]])
          )
        }

    private def trusted(c: Class[_]): Boolean = validator.forall { case (v, isTrusted) =>
      isTrusted.invoke(v, c).asInstanceOf[Boolean]
    }

    private val outer = Outer(
      "o",
      Inner(new BigDecimal("1.50")),
      List(Item("a"), Item("b")).asJava,
      Map("k" -> Item("c")).asJava
    )
    outer.base = Item("from superclass")

    "ReflectRecordDatumFactory" should "collect the record's classes recursively" in {
      val classes = ReflectRecordDatumFactory.recordClasses(classOf[Outer])
      classes should contain allOf (
        classOf[Outer],
        classOf[Inner], // field
        classOf[Item], // type argument and superclass field
        classOf[BigDecimal], // nested field
        classOf[java.util.List[_]],
        classOf[java.util.Map[_, _]],
        classOf[String]
      )
      classes should not contain classOf[Unrelated] // transient field
      classes should not contain classOf[Object]
      // avro never loads a superclass by name, only its fields' classes
      classes should not contain classOf[Base]
    }

    it should "collect array components and @Union subtypes, and skip @AvroIgnore" in {
      val classes = ReflectRecordDatumFactory.recordClasses(classOf[WithAnnotations])
      classes should contain allOf (classOf[Item], classOf[Shape], classOf[Circle], classOf[Square])
      classes should not contain classOf[Inner] // @AvroIgnore
    }

    it should "roundtrip nested reflect records without trusting their packages" in {
      implicit val coder: Coder[Outer] = avroReflectCoder[Outer]
      outer coderShould roundtrip()
    }

    it should "trust exactly the record's classes" in {
      assume(validator.isDefined, "avro has no ClassSecurityValidator")
      new ReflectRecordDatumFactory(classOf[Outer])(Outer.schema, Outer.schema)
      trusted(classOf[Outer]) shouldBe true
      trusted(classOf[Inner]) shouldBe true
      trusted(classOf[Item]) shouldBe true
      // same package as Inner and Item, not reachable from Outer
      trusted(classOf[Unrelated]) shouldBe false
    }
  }

  class Base {
    var base: Item = _
  }

  case class Outer(
    var name: String,
    var inner: Inner,
    var items: java.util.List[Item],
    var byKey: java.util.Map[String, Item]
  ) extends Base {
    def this() = this(null, null, null, null)
    @transient var cache: Unrelated = _
  }

  object Outer {
    lazy val schema: org.apache.avro.Schema =
      org.apache.avro.reflect.ReflectData.get().getSchema(classOf[Outer])
  }

  class Shape
  class Circle extends Shape
  class Square extends Shape

  class WithAnnotations {
    var items: Array[Item] = _
    @org.apache.avro.reflect.Union(Array(classOf[Circle], classOf[Square]))
    var shape: Shape = _
    @org.apache.avro.reflect.AvroIgnore
    var ignored: Inner = _
  }
}

package com.spotify.scio.avro.reflectautotrust.other {
  case class Inner(var amount: java.math.BigDecimal) { def this() = this(null) }
  case class Item(var name: String) { def this() = this(null) }
  class Unrelated(var name: String) { def this() = this(null) }
}
