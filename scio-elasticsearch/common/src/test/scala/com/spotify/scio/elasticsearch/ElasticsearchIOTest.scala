/*
 * Copyright 2022 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.elasticsearch

import co.elastic.clients.elasticsearch.core.bulk.{BulkOperation, IndexOperation}
import co.elastic.clients.json.jackson.{
  JacksonJsonpGenerator,
  JacksonJsonpMapper,
  JacksonJsonpParser
}
import com.fasterxml.jackson.core.JsonFactory
import com.spotify.scio.testing._

import java.io.StringWriter
import java.time.LocalDate

object ElasticsearchIOTest {
  type Document = Map[String, String]
  case class Record(i: Int, jt: java.time.Instant, ldt: LocalDate)
}

class ElasticsearchIOTest extends ScioIOSpec {
  import ElasticsearchIOTest._

  "ElasticsearchIO" should "work with output" in {
    val xs = 1 to 100

    def opts(): ElasticsearchOptions = ElasticsearchOptions(Nil)

    testJobTestOutput(xs)(_ => ElasticsearchIO(opts())) { case (data, _) =>
      data.saveAsElasticsearch(opts())(id =>
        Seq(
          BulkOperation.of(_.index(IndexOperation.of[Document](_.index("index").id(id.toString))))
        )
      )
    }
  }

  "ElasticsearchOptions" should "have a default mapper that supports scala and java.time" in {
    val record = Record(10, java.time.Instant.ofEpochSecond(10), LocalDate.of(2021, 10, 22))

    // the mapper internals require that the generator and parser types match,
    // so we have to do some awkward java dancing here
    val mapper = ElasticsearchOptions(Nil).mapperFactory()
    val writer = new StringWriter()
    val generator = new JacksonJsonpGenerator(new JsonFactory().createGenerator(writer))
    mapper.serialize(record, generator)
    generator.close()
    val parser = new JacksonJsonpParser(
      new JsonFactory().createParser(writer.toString),
      mapper.asInstanceOf[JacksonJsonpMapper]
    )
    val roundTripped = mapper.deserialize(parser, classOf[Record])

    roundTripped should equal(record)
  }
}
