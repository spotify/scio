package com.spotify.scio.examples.common

import com.google.cloud.dataflow.examples.common.{ExamplePubsubTopicOptions, ExampleBigQueryTableOptions}
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions

trait ExampleOptions extends DataflowPipelineOptions with ExampleBigQueryTableOptions with ExamplePubsubTopicOptions
