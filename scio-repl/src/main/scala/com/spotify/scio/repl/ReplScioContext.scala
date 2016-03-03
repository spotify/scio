package com.spotify.scio.repl

import java.io.{OutputStream, PrintStream}

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.spotify.scio.ScioResult

import scala.concurrent.Future

class ReplScioContext (options: DataflowPipelineOptions, private var artifacts: List[String], testId: Option[String])
  extends com.spotify.scio.ScioContext (options, artifacts, testId) {

  private val nullout = new PrintStream(new OutputStream() {
    override def write(b: Int) = {}
    override def write(b: Array[Byte]) = {}
    override def write(b: Array[Byte], off: Int, len: Int) = {}
  })

  /**
   * Async close/run Scio context.
   * NOTE: by default no std{out,err} is available as feedback
   * NOTE: to turn off or reduce logging noise - use sl4j properties - for example:
   *  `-Dorg.slf4j.simpleLogger.logFile=/dev/null`
   */
  def asyncClose(stdout: PrintStream = nullout, stderr: PrintStream = nullout): Future[ScioResult] = {
    import scala.language.reflectiveCalls
    this.getClass.getClassLoader.asInstanceOf[ {def createReplCodeJar: String} ].createReplCodeJar
    import scala.concurrent.ExecutionContext.Implicits.global

    /* Console.with* affects worker thread - dataflow version/job id manages to get printed still */
    Future(Console.withOut(stdout){
      Console.withErr(stderr) {
        super.close()
      }
    })
  }

  /** Enhance original close method with dumping REPL session jar */
  override def close(): ScioResult = {
    import scala.language.reflectiveCalls
    /*TODO: add notification if distributed mode, that it may take minutes for DF to start */
    this.getClass.getClassLoader.asInstanceOf[ {def createReplCodeJar: String} ].createReplCodeJar
    super.close()
  }
}
