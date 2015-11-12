package com.spotify.scio.examples.common

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.examples.common.{ExampleBigQueryTableOptions, ExamplePubsubTopicOptions}
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions

trait ExampleOptions extends DataflowPipelineOptions with ExampleBigQueryTableOptions with ExamplePubsubTopicOptions

object ExampleOptions {
  def bigQueryTable(options: ExampleOptions): String =
    BigQueryIO.toTableSpec(new TableReference()
      .setProjectId(options.getProject)
      .setDatasetId(options.getBigQueryDataset)
      .setTableId(options.getBigQueryTable))
}