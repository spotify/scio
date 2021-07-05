package com.spotify.scio.memcached

import com.spotify.folsom.MemcacheClient
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, Setup, Teardown}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection, PDone}



object MemcacheIOTransform{

  def write(memcacheConnectionOptions: MemcacheConnectionOptions): Write =  {
    Write(memcacheConnectionOptions)
  }

  case class Write(memcacheConnectionOptions: MemcacheConnectionOptions) extends PTransform[PCollection[KV[String, String]], PDone] {
    override def expand(input: PCollection[KV[String, String]]): PDone = {
      input.apply(ParDo.of(MemcacheWriterFn(memcacheConnectionOptions)))
      PDone.in(input.getPipeline)
    }
  }

  case class MemcacheWriterFn(memcacheConnectionOptions: MemcacheConnectionOptions) extends DoFn[KV[String, String], Void] {

    lazy val memcacheClient: MemcacheClient[String] = MemcacheConnectionOptions.connect(memcacheConnectionOptions)

//    This might not be necessary as the client above would be created when the class is created
//    @Setup
//    def setup(): Unit = ???

    @ProcessElement
    def processElement(processContext: ProcessContext):Unit = {
      val record: KV[String, String] = processContext.element()
      memcacheClient.add(record.getKey, record.getValue, memcacheConnectionOptions.ttl)
    }

    @FinishBundle
    def finishBundle(): Unit = memcacheClient.flushAll(memcacheConnectionOptions.flushDelay)

    @Teardown
    def teardown(): Unit = {
      memcacheClient.flushAll(memcacheConnectionOptions.flushDelay)
    }

  }
}
