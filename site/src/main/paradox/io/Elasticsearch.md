# Elasticsearch

Scio supports writing to [Elasticsearch](https://github.com/elastic/elasticsearch).

## Writes

An `SCollection` of arbitrary elements can be saved to Elasticsearch with
@scaladoc[saveAsElasticsearch](com.spotify.scio.elasticsearch.ElasticsearchSCollection#saveAsElasticsearch(esOptions:com.spotify.scio.elasticsearch.ElasticsearchOptions,flushInterval:org.joda.time.Duration,numOfShards:Long,maxBulkRequestOperations:Int,maxBulkRequestBytes:Long,errorFn:org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException=%3EUnit,retry:com.spotify.scio.elasticsearch.ElasticsearchIO.RetryConfig)(f:T=%3EIterable[co.elastic.clients.elasticsearch.core.bulk.BulkOperation]):com.spotify.scio.io.ClosedTap[Nothing]).
The @scaladoc[ElasticsearchOptions](com.spotify.scio.elasticsearch.ElasticsearchOptions)-typed `esOptions` argument requires a `mapperFactory` argument capable of mapping the element type to json.
`saveAsElasticsearch` takes a second argument list, whose single argument `f` can be provided as a block, and which maps the input type to Elasticsearch [BulkOperations](https://artifacts.elastic.co/javadoc/co/elastic/clients/elasticsearch-java/8.8.0/co/elastic/clients/elasticsearch/core/bulk/BulkOperation.html).

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.elasticsearch._

import co.elastic.clients.elasticsearch.core.bulk.{BulkOperation, IndexOperation}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.apache.http.HttpHost
import java.time.LocalDate

val host: String = ???
val port: Int = ???
val esIndex: String = ???

case class Document(user: String, postDate: LocalDate, word: String, count: Long)

val primaryESHost = new HttpHost(host, port)
val mapperFactory = () => {
  val mapper = new JacksonJsonpMapper()
  mapper.objectMapper().registerModule(DefaultScalaModule)
  mapper.objectMapper().registerModule(new JavaTimeModule())
  mapper
}
val esOptions = ElasticsearchOptions(
  nodes = Seq(primaryESHost), 
  mapperFactory = mapperFactory
)

val elements: SCollection[Document] = ???
elements.saveAsElasticsearch(esOptions) { d =>
  List(
    BulkOperation.of { bulkBuilder =>
      bulkBuilder.index(
        IndexOperation.of[Document] { indexBuilder =>
          indexBuilder
            .index(esIndex)
            .document(d)
        }
      )
    }
  )
}
```
