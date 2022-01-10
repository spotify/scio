package com.spotify.scio.examples

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.extra.{AvroExample, ParquetExample}
import com.spotify.scio.util.ScioUtil
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object RunPreReleaseIT {

  implicit private val ec: ExecutionContext = ExecutionContext.global

  private val defaultArgs = Array(
    "--runner=DataflowRunner",
    "--project=data-integration-test",
    "--region=europe-west1",
    "--tempLocation=gs://dataflow-tmp-europe-west1/gha"
  )

  private val log = LoggerFactory.getLogger(getClass)

  def main(cmdLineArgs: Array[String]): Unit = {
    val (_, args) = ContextAndArgs(cmdLineArgs)
    val runId = args("runId")

    Await.result(
      Future
        .sequence(
          parquet(runId) :+ avro(runId)
        )
        .map(_ => log.info("All Dataflow jobs ran successfully."))
        .recover(e => log.error("At least one Dataflow job failed", e)),
      Duration(1, TimeUnit.HOURS)
    )
  }

  private def avro(runId: String): Future[Unit] = {
    val out1 = gcsPath[AvroExample.type]("specificOut", runId)
    val out2 = gcsPath[AvroExample.type]("specificIn", runId)

    Future
      .successful(log.info("Starting Avro IO tests... "))
      .flatMap(_ => invokeJob[AvroExample.type]("--method=specificOut", s"--output=$out1"))
      .flatMap(_ =>
        invokeJob[AvroExample.type]("--method=specificIn", s"--input=$out1/*", s"--output=$out2")
      )
  }

  private def parquet(runId: String): List[Future[Unit]] = {
    val out1 = gcsPath[ParquetExample.type]("avroOut", runId)
    val out2 = gcsPath[ParquetExample.type]("typedIn", runId)
    val out3 = gcsPath[ParquetExample.type]("avroSpecificIn", runId)

    val write = Future
      .successful(log.info("Starting Parquet IO tests... "))
      .flatMap(_ => invokeJob[ParquetExample.type]("--method=avroOut", s"--output=$out1"))

    List(
      write.flatMap(_ =>
        invokeJob[ParquetExample.type]("--method=typedIn", s"--input=$out1/*", s"--output=$out2")
      ),
      write.flatMap(_ =>
        invokeJob[ParquetExample.type](
          "--method=avroSpecificIn",
          s"--input=$out1/*",
          s"--output=$out3"
        )
      )
    )
  }

  private def invokeJob[T: ClassTag](args: String*): Future[Unit] =
    Future {
      val cls = ScioUtil.classOf[T]
      val jobObjName = cls.getName.replaceAll("\\$$", "")
      log.info(s"Running Dataflow job ${cls.getName}...")
      Try(
        Class
          .forName(jobObjName)
          .getMethod("main", classOf[Array[String]])
          .invoke(null, defaultArgs ++ Array(args: _*))
      ) match {
        case Success(_) => log.info(s"Dataflow job ${cls.getName} ran successfully.")
        case Failure(e) =>
          throw new RuntimeException(s"Dataflow job ${cls.getName} failed with ${e.getClass}", e)
      }
    }

  private def gcsPath[T: ClassTag](methodName: String, runId: String): String =
    s"gs://data-integration-test-prerelease-it/${ScioUtil.classOf[T].getSimpleName}/$methodName/$runId"
}
