/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.values

import com.spotify.scio.ScioContext
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.errorhandling.{BadRecord, ErrorHandler}
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple, TupleTag}

/**
 * A sink for error records.
 *
 * Once the [[sink]] is materialized, the [[handler]] must not be used anymore.
 */
sealed trait ErrorSink {
  def handler: ErrorHandler[BadRecord, _]
  def sink: SCollection[BadRecord]
}

object ErrorSink {

  private class SinkSideOutput(tag: TupleTag[BadRecord])
      extends PTransform[PCollection[BadRecord], PCollectionTuple] {
    override def expand(input: PCollection[BadRecord]): PCollectionTuple =
      PCollectionTuple.of(tag, input)
  }

  private[scio] def apply(context: ScioContext): ErrorSink = {
    new ErrorSink {
      private val tupleTag: TupleTag[BadRecord] = new TupleTag[BadRecord]()

      override val handler: ErrorHandler[BadRecord, PCollectionTuple] =
        context.pipeline.registerBadRecordErrorHandler(new SinkSideOutput(tupleTag))

      override def sink: SCollection[BadRecord] = {
        handler.close()
        val output = handler.getOutput
        context.wrap(output.get(tupleTag))
      }
    }
  }
}
