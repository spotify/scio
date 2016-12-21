package com.spotify.scio.util

import com.spotify.scio.testing.PipelineSpec

class CallSitesTest extends PipelineSpec {

  it should "generate deterministically name with same operation" in {
    runWithData(Seq("1")) { p =>
      val map1 = p.map(s => s.toInt)
      val map2 = map1.map(i => i * 2)
      val map3 = map2.map(i => i.toString)

      map1.name should be("map#1.out")
      map2.name should be("map#2.out")
      map3.name should be("map#3.out")

      map3
    } should equal(Seq("2"))
  }

  it should "generate deterministically name with different operations" in {
    runWithData(Seq(Seq(1, 2), Seq(3, 4))) { p =>
      val flatMap = p.flatMap(s => s)
      val sum = flatMap.sum

      flatMap.name should be("flatMap#1.out")
      sum.name should be("sum#1/Values/Values.out")

      sum
    } should equal(Seq(10))
  }


}
