package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.avro.{Account, TestRecord}

object AvroInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.avroFile[TestRecord](args("input"))
      .map(r => new Account(r.getIntField, "checking", r.getStringField, r.getDoubleField))
      .saveAsAvroFile(args("output"))
    sc.close()
  }
}
