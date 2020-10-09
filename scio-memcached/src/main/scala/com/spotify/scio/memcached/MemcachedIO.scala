package com.spotify.scio.memcached

import com.spotify.scio.ScioContext
import com.spotify.scio.io._
import com.spotify.scio.memcache.MemcacheIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.values.KV

case class MemcacheConnectionSettings(hostname: String, ttl: Int, flushDelay: Int, connectWaitSeconds: Int)
//The ints might have to be a like scala concurrent seconds or something

sealed trait MemcacheIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
}

final class MemcacheIOWrite(connectionSetting: MemcacheConnectionSettings) extends MemcacheIO[(String, String)] {
  override type ReadP = Nothing
  override type WriteP = MemcacheIOWrite

  override def tap(read: ReadP): Tap[Nothing] = EmptyTap

  override protected def read(sc: ScioContext, params: ReadP): SCollection[(String, String)] =
    throw new UnsupportedOperationException("cannot write to Memcached yet")

  override protected def write(
    data: SCollection[(String, String)],
    params: WriteP
  ): Tap[Nothing] = {

    val dataTmp = data.map{ case (k, v) => KV.of(k,v)}

    dataTmp.applyInternal(MemcacheIO.write())

    EmptyTap

  }
}

object MemcacheIOWrite {
  object WriteParam {
    private[memcached] val DefaultConnectWaitSeconds = 10
//    private [memcached] val Defautl
  }
//  awaitDisconnected
//  withMaxOutstandingRequests
// default port
//  default ttl
//
}

//object MemeCachedIO {
//
//  def write(connectionSettings: MemcacheConnectionSettings): Write = new Write(connectionSettings)
//
//  @AutoValue
//  class Write(connectionSettings: MemcacheConnectionSettings) extends PTransform[PCollection[KV[String, String]], PDone] {
//
//    lazy val client: MemcacheClient[String] = Write.client(connectionSettings)
//
//    @Override
//    def expand(input: PCollection[KV[String, String]]): PDone = {
//      input.apply(ParDo.of(new WriteFn))
//      PDone.in(input.getPipeline)
//    }
//
//    class WriteFn extends DoFn[KV[String, String], Unit] {
//      @Setup
//      def setup(): Unit =
//        client.awaitFullyConnected(connectionSettings.connectWaitSeconds, TimeUnit.SECONDS)
//
//      @StartBundle
//      def startBundle(): Unit = {
//        client
//      }
//
//      @ProcessElement
//      def processElement(processContext: ProcessContext): Unit = {
//        val element: KV[String, String] = processContext.element()
////        There are few more add methods, have to figure out what to use
//        val tmp: CompletionStage[MemcacheStatus] = client.add(element.getKey, element.getValue, connectionSettings.ttl)
////        Now this has to be blocking call or how do we handled this this
//
//      }
//
//      @FinishBundle
//      def finishBundle(): Unit = {
//        client.flushAll(connectionSettings.flushDelay)
//      }
//
//      @Teardown
//      def teardown(): Unit =
//        client.flushAll(connectionSettings.flushDelay)
//        client.shutdown()
//
//    }
//  }
//
//  object Write {
////    we want this to be lazy we dont want to be creating new client on very process element
//    def client(connectionSettings: MemcacheConnectionSettings): MemcacheClient[String] = {
//      MemcacheClientBuilder
//        .newStringClient()
//        .withAddress(connectionSettings.hostname)
//        .connectAscii()
//    }
//  }
//}
