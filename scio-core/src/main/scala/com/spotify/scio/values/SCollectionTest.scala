"unionAll companion object" should "throw descriptive error on empty collection" in {
  val empty = Seq.empty[SCollection[Int]]
  val exception = the[IllegalArgumentException] thrownBy {
    SCollection.unionAll(empty)
  }
  exception.getMessage should include("ScioContext#unionAll")
}

"unionAll companion object" should "handle single collection" in {
  runWithContext { sc =>
    val single = sc.parallelize(Seq(1, 2, 3))
    val result = SCollection.unionAll(Seq(single))
    result should containInAnyOrder(Seq(1, 2, 3))
  }
}

"unionAll companion object" should "handle multiple collections" in {
  runWithContext { sc =>
    val sc1 = sc.parallelize(Seq(1, 2))
    val sc2 = sc.parallelize(Seq(3, 4))
    val sc3 = sc.parallelize(Seq(5, 6))
    val result = SCollection.unionAll(Seq(sc1, sc2, sc3))
    result should containInAnyOrder(Seq(1, 2, 3, 4, 5, 6))
  }
}
"unionAll context method" should "handle empty collection safely" in {
  runWithContext { sc =>
    val empty = Seq.empty[SCollection[Int]]
    val result = sc.unionAll(empty)
    result should beEmpty
  }
}

"unionAll context method" should "handle multiple collections" in {
  runWithContext { sc =>
    val sc1 = sc.parallelize(Seq(1, 2))
    val sc2 = sc.parallelize(Seq(3, 4))
    val result = sc.unionAll(Seq(sc1, sc2))
    result should containInAnyOrder(Seq(1, 2, 3, 4))
  }
}
