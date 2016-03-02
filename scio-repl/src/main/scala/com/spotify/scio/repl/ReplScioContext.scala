package com.spotify.scio.repl

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.spotify.scio.ScioResult

class ReplScioContext (options: DataflowPipelineOptions, private var artifacts: List[String], testId: Option[String])
  extends com.spotify.scio.ScioContext (options, artifacts, testId) {
  /** Enhance original close method with dumping REPL session jar */
  override def close(): ScioResult = {
    import scala.language.reflectiveCalls
    /*TODO: add notification if distributed mode, that it may take minutes for DF to start */
    this.getClass.getClassLoader.asInstanceOf[ {def createReplCodeJar: String} ].createReplCodeJar
    super.close()
  }
}
