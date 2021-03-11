package com.spotify.scio.extra.rollup

import com.spotify.scio.extra.rollup.RollupTest._
import com.spotify.scio.testing._
import com.twitter.algebird.Group
import com.twitter.algebird.macros.caseclass

object RollupTest {

  implicit val ccg: Group[MsPlayed] = caseclass.group

  case class FixedDims(date: String, country: String)
  case class RollupDims1D(platform: Option[String])
  case class RollupDims2D(platform: Option[String], os: Option[String])
  case class MsPlayed(value: Long)

  def groupingSets(dims: RollupDims1D): Set[RollupDims1D] =
    (for {
      newDims <- List(dims.copy(platform = None), dims)
    } yield newDims).toSet

  def groupingSets(dims: RollupDims2D): Set[RollupDims2D] =
    (for {
      allPlatforms <- List(dims.copy(platform = None), dims)
      allCombos <- List(allPlatforms.copy(os = None), allPlatforms)
    } yield allCombos).toSet

}

class RollupTest extends PipelineSpec {

  "it" should "work with empty input" in {

    val input = Seq[(String, FixedDims, RollupDims1D, MsPlayed)]()

    runWithData(input)(_.rollupAndCount(groupingSets)) should contain theSameElementsAs Seq()
  }

  "it" should "not double-count a user with multiple values of rollup dimensions" in {

    val input = Seq(
      ("user1", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("web")), MsPlayed(100L)),
      ("user1", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("mobile")), MsPlayed(200L))
    )

    val expected = Seq(
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("web"))), (MsPlayed(100L), 1L)),
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("mobile"))), (MsPlayed(200L), 1L)),
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(None)), (MsPlayed(300L), 1L))
    )

    runWithData(input)(_.rollupAndCount(groupingSets)) should contain theSameElementsAs expected
  }

  "it" should "correctly sum users on top-level even if they are active in different cohorts" in {

    val input = Seq(
      ("user1", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("web")), MsPlayed(100L)),
      ("user1", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("mobile")), MsPlayed(200L)),
      ("user2", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("speaker")), MsPlayed(200L))
    )

    val expected = Seq(
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("web"))), (MsPlayed(100L), 1L)),
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("mobile"))), (MsPlayed(200L), 1L)),
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("speaker"))), (MsPlayed(200L), 1L)),
      ((FixedDims("2020-01-01", "sweden"), RollupDims1D(None)), (MsPlayed(500L), 2L))
    )

    runWithData(input)(_.rollupAndCount(groupingSets)) should contain theSameElementsAs expected
  }

  "it" should "correctly separate on fixed dimensions and not sum users with the same rolllup " +
    "dimensions" in {

      val input = Seq(
        ("user1", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("web")), MsPlayed(100L)),
        ("user1", FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("mobile")), MsPlayed(200L)),
        ("user2", FixedDims("2020-01-02", "sweden"), RollupDims1D(Some("speaker")), MsPlayed(200L))
      )

      val expected = Seq(
        ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("web"))), (MsPlayed(100L), 1L)),
        ((FixedDims("2020-01-01", "sweden"), RollupDims1D(Some("mobile"))), (MsPlayed(200L), 1L)),
        ((FixedDims("2020-01-02", "sweden"), RollupDims1D(Some("speaker"))), (MsPlayed(200L), 1L)),
        ((FixedDims("2020-01-01", "sweden"), RollupDims1D(None)), (MsPlayed(300L), 1L)),
        ((FixedDims("2020-01-02", "sweden"), RollupDims1D(None)), (MsPlayed(200L), 1L))
      )

      runWithData(input)(_.rollupAndCount(groupingSets)) should contain theSameElementsAs expected
    }

  "it" should "correctly sum users on matching rolled up dimensions (android, total)" in {

    val input = Seq(
      (
        "user1",
        FixedDims("2020-01-01", "sweden"),
        RollupDims2D(Some("web"), Some("linux")),
        MsPlayed(100L)
      ),
      (
        "user1",
        FixedDims("2020-01-01", "sweden"),
        RollupDims2D(Some("mobile"), Some("android")),
        MsPlayed(200L)
      ),
      (
        "user2",
        FixedDims("2020-01-01", "sweden"),
        RollupDims2D(Some("tablet"), Some("android")),
        MsPlayed(300L)
      )
    )

    val expected = Seq(
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(Some("web"), Some("linux"))),
        (MsPlayed(100L), 1L)
      ),
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(Some("mobile"), Some("android"))),
        (MsPlayed(200L), 1L)
      ),
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(Some("tablet"), Some("android"))),
        (MsPlayed(300L), 1L)
      ),
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(None, Some("linux"))),
        (MsPlayed(100L), 1L)
      ),
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(None, Some("android"))),
        (MsPlayed(500L), 2L)
      ),
      ((FixedDims("2020-01-01", "sweden"), RollupDims2D(Some("web"), None)), (MsPlayed(100L), 1L)),
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(Some("mobile"), None)),
        (MsPlayed(200L), 1L)
      ),
      (
        (FixedDims("2020-01-01", "sweden"), RollupDims2D(Some("tablet"), None)),
        (MsPlayed(300L), 1L)
      ),
      ((FixedDims("2020-01-01", "sweden"), RollupDims2D(None, None)), (MsPlayed(600L), 2L))
    )

    runWithData(input)(_.rollupAndCount(groupingSets)) should contain theSameElementsAs expected
  }

}
