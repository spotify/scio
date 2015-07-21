package com.spotify.cloud.dataflow.examples.extra

import com.spotify.cloud.dataflow._
import com.spotify.cloud.dataflow.avro.{Account, TestRecord}

object AvroInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    context.avroFile[TestRecord](args("input"))
      .map(r => new Account(r.getIntField, "checking", r.getStringField, r.getDoubleField))
      .saveAsAvroFile(args("output"))
    context.close()
  }
}
