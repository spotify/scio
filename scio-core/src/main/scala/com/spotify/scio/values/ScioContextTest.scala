"ScioContext.unionAll" should "handle empty collection safely" in {
  runWithContext { sc =>
    val empty = Seq.empty[SCollection[Int]]
    val result = sc.unionAll(empty)
    result should beEmpty
  }
}

"ScioContext.unionAll" should "handle multiple collections" in {
  runWithContext { sc =>
    val sc1 = sc.parallelize(Seq(1, 2))
    val sc2 = sc.parallelize(Seq(3, 4))
    val result = sc.unionAll(Seq(sc1, sc2))
    result should containInAnyOrder(Seq(1, 2, 3, 4))
  }
}