package fix.v0_8_0

import com.spotify.scio.ScioResult
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.PipelineResult.State

class ValidateInvoicePaymentsJobTest extends PipelineSpec {

  def doSomething(s: ScioResult): Unit = ()
  def doSomethingElse(s: ScioResult, x: String): Unit = ()
  def doSomethingWithAString(s: String): Unit = ()
  def maybeReturnAnInt(s: ScioResult, x: String): Option[Int] = Option(1)

  it should "test something" in {
    val r = runWithContext(_.parallelize(Seq(1, 2, 3)))
    r.isCompleted shouldBe true
    r.state shouldBe State.DONE
    doSomething(r.waitUntilFinish())
    doSomethingElse(r.waitUntilFinish(), "hello") shouldBe ()
    doSomethingElse(r.waitUntilFinish(), "hello") should
      be()

    val r2 = runWithContext { sc =>
      {
        ()
      }
    }
    maybeReturnAnInt(r2.waitUntilFinish(), "yolo") should be(Some(1))

    def scioResultFn(s: String): ScioResult = {
      runWithContext { sc =>
        {
          ()
        }
      }
    }.waitUntilFinish()
  }

  it should "test something else" in runWithContext { sc =>
    val coll = sc.parallelize(Seq(1, 2, 3))
    val r = "Hello World"
    doSomethingWithAString(r)
    coll should haveSize(3)
  }
}
