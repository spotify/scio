package com.spotify.scio.elasticsearch

import com.spotify.scio.testing.PipelineSpec
class ElasticsearchIOIT extends PipelineSpec with ElasticsearchIOBehavior {
  "Elasticsearch v7" should behave like elasticsearchIO()
}
