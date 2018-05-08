package com.spotify.scio.util

import java.util.concurrent.Semaphore

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Utility class to limit the number of parallel doFns
 * @param maxDoFns Max number of doFns
 */
private[scio] abstract class ParallelLimitedFn[T, U](maxDoFns: Int)
  extends DoFn[T, U] with NamedFn {

  private val semaphore: Semaphore = new Semaphore(maxDoFns, true)

  def parallelProcessElement(x: DoFn[T, U]#ProcessContext): Unit

  @ProcessElement def processElement(x: DoFn[T, U]#ProcessContext): Unit = {
    try {
      semaphore.acquire()
      parallelProcessElement(x)
    } finally {
      semaphore.release()
    }
  }
}
