/*
 * Copyright 2023 Spotify AB
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

package org.apache.beam.sdk.io.gcp.bigquery

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, TupleTag}
import org.mockito.MockitoSugar.mock

object mockUtils {
  def mockedWriteResult(
    pipeline: Pipeline = mock[Pipeline],
    failedInsertsTag: TupleTag[TableRow] = mock[TupleTag[TableRow]],
    failedInserts: PCollection[TableRow] = mock[PCollection[TableRow]]
  ) = {
    WriteResult.in(
      pipeline,
      failedInsertsTag,
      failedInserts,
      null,
      null,
      null,
      null,
      null
    )
  }
}
