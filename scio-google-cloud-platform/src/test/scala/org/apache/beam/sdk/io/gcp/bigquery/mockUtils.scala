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
