package com.spotify.scio.elasticsearch

import co.elastic.clients.json.{JsonpMapper, SimpleJsonpMapper}
import org.apache.http.HttpHost

final case class ElasticsearchOptions(
  nodes: Seq[HttpHost],
  usernameAndPassword: Option[(String, String)] = None,
  mapperFactory: () => JsonpMapper = () => new SimpleJsonpMapper()
)
