//package com.spotify.scio.cosmosdb.read
//
//import com.azure.data.tables.models.{ ListEntitiesOptions, TableEntity }
//import com.azure.data.tables.{ TableServiceAsyncClient, TableServiceClientBuilder }
//import org.apache.beam.sdk.annotations.Experimental
//import org.apache.beam.sdk.annotations.Experimental.Kind
//import org.apache.beam.sdk.io.BoundedSource
//import org.slf4j.LoggerFactory
//
//@Experimental(Kind.SOURCE_SINK)
//class TableStorageBoundedReader(tableStorageBoundedSource: TableStorageBoundedSource)
//    extends BoundedSource.BoundedReader[TableEntity] {
//  private val log = LoggerFactory.getLogger(getClass)
//  private var maybeTableServiceAsyncClient: Option[TableServiceAsyncClient] = None
//  private var maybeIterator: Option[java.util.Iterator[TableEntity]] = None
//
//  override def start(): Boolean = {
//    log.debug("TableStorageBoundedReader.start()")
//
//    maybeTableServiceAsyncClient = Some(
//      new TableServiceClientBuilder()
//        .endpoint(tableStorageBoundedSource.tableStorageRead.endpoint)
//        .sasToken(tableStorageBoundedSource.tableStorageRead.sasToken)
//        .buildAsyncClient()
//    )
//
//    maybeIterator = maybeTableServiceAsyncClient.map { tableServiceAsyncClient =>
//      log.info("Get the container name")
//      log.info(s"Get the iterator of the query in container ${tableStorageBoundedSource.tableStorageRead.tableName}")
//
//      val listEntitiesOptions = new ListEntitiesOptions
//
//      val query = tableStorageBoundedSource.tableStorageRead.query
//      if (query != null && !query.isBlank) {
//        listEntitiesOptions.setFilter(query)
//      }
//
//      tableServiceAsyncClient
//        .getTableClient(tableStorageBoundedSource.tableStorageRead.tableName)
//        .listEntities(listEntitiesOptions)
//        .toIterable
//        .iterator()
//    }
//
//    maybeIterator.isDefined
//  }
//
//  override def advance(): Boolean = maybeIterator.exists(_.hasNext)
//
//  override def getCurrent: TableEntity =
//    maybeIterator
//      .filter(_.hasNext)
//      .map(_.next())
//      .orNull
//
//  override def getCurrentSource: TableStorageBoundedSource = tableStorageBoundedSource
//
//  override def close(): Unit = ()
//}
