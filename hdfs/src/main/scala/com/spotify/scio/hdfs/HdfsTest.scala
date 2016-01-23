package com.spotify.scio.hdfs

import com.spotify.lambda.records.Vector
import com.spotify.scio._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object HdfsTest {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    readAvro(sc)
//    readText(sc)

//    val output = args("output")
//    writeAvro(sc, output)
//    writeText(sc, output)

    sc.close()
  }

  def readAvro(sc: ScioContext): Unit = {
    sc.hdfsAvroFile[Vector]("hdfs:///user/neville/scio/avro")
      .map{ s => println(s); s }
      .map(identity)
  }

  def readText(sc: ScioContext): Unit = {
    sc.hdfsTextFile("hdfs:///user/neville/scio/text")
      .map{ s => println(s); s }
  }

  def writeAvro(sc: ScioContext, output: String): Unit = {
    val job = Job.getInstance()
    AvroJob.setOutputKeySchema(job, Vector.getClassSchema)

    sc.parallelize(1 to 100)
      .map { i =>
        Vector.newBuilder()
          .setGid("gid" + i)
          .setVector(List.fill(10)(i.toFloat.asInstanceOf[java.lang.Float]).asJava)
          .setVersion("v1")
          .build()
      }
      .saveAsHdfsAvroFile(output)
  }

  def writeText(sc: ScioContext, output: String): Unit = {
    sc.textFile("README.md")
      .saveAsHdfsTextFile(output)
  }

  object Avros {
    def copySpecificRecord[T <: AnyRef](obj: T)(implicit ct: ClassTag[T]): T = {
      val builder = ct.runtimeClass.getMethod("newBuilder", ct.runtimeClass).invoke(null, obj)
      builder.getClass.getMethod("build").invoke(builder).asInstanceOf[T]
    }
  }
}
