package com.spotify.scio.testing
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.schemas.Schema
import com.spotify.scio.testing.util.SCollectionPrettifier
import org.scalactic.Prettifier

class SCollectionPrettifierTest extends PipelineSpec {
  behavior of "SCollectionPrettifier"

  it should "prettify should containInAnyOrder(expected) matcher" in {
    val actual = (1 to 2).map(a => TestRecord.newBuilder().setIntField(a).build())
    val incorrectExpected = (1 to 1).map(a => TestRecord.newBuilder().setIntField(a).build())

    val thrown = intercept[AssertionError] {
      runWithContext {
        _.parallelize(actual) should containInAnyOrder(incorrectExpected)
      }
    }
    // Test to make sure that our specialized schema based prettifier
    // is being used by containInAnyOrder
    val prettifier =
      SCollectionPrettifier.getPrettifier(
        Schema[TestRecord],
        fallbackPrettifier = Prettifier.default
      )

    val expectedStringInErrorMessage = prettifier(incorrectExpected)

    thrown.getMessage should include(expectedStringInErrorMessage)
  }

  it should "prettify shouldNot containInAnyOrder(expected) matcher" in {
    val actual = (1 to 1).map(a => TestRecord.newBuilder().setIntField(a).build())
    val correctExpected = (1 to 1).map(a => TestRecord.newBuilder().setIntField(a).build())

    val thrown = intercept[AssertionError] {
      runWithContext {
        _.parallelize(actual) shouldNot containInAnyOrder(correctExpected)
      }
    }
    // Test to make sure that our specialized schema based prettifier
    // is being used by containInAnyOrder
    val prettifier =
      SCollectionPrettifier.getPrettifier(
        Schema[TestRecord],
        fallbackPrettifier = Prettifier.default
      )

    val expectedStringInErrorMessage = prettifier(correctExpected)

    thrown.getMessage should include(expectedStringInErrorMessage)
  }
}
