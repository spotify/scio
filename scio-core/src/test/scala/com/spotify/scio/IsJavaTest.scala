package com.spotify.scio

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IsJavaTest extends AnyFlatSpec with Matchers {
  "IsJavaBean" should "succeed for a java bean" in {
    println(IsJavaBean[JavaBeanA])
  }

  it should "not compile for a case class" in {
    case class Foo(s: String, i: Int)
    "IsJavaBean[Foo]" shouldNot compile
  }
}
