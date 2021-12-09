package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AsyncLookupDoFnExample {

  class HttpLookupDofn(val uri: String) extends ScalaAsyncLookupDoFn[Int, Int, HttpClient] {
    override def asyncLookup(
      client: HttpClient,
      input: Int
    ): Future[Int] =
      Future(
        client.execute(new HttpGet(s"$uri/$input")).getStatusLine.getStatusCode
      )(ExecutionContext.global)

    override protected def newClient(): HttpClient = HttpClientBuilder.create().build()
  }

  private lazy val log = LoggerFactory.getLogger(getClass)

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    val successfulCalls = sc.initCounter("successfulCalls")
    val failedCalls = sc.initCounter("failedCalls")

    sc
      .customInput(
        "statusCodes",
        GenerateSequence
          .from(100)
          .withRate(1, Duration.standardSeconds(5))
      )
      .map(i => (i % 600).toInt)
      .parDo(new HttpLookupDofn(args.getOrElse("host", "https://httpstat.us")))
      .map { i =>
        val req = i.getKey
        i.getValue match {
          case Success(response) =>
            successfulCalls.inc()
            log.info(s"Successful call: <$req, $response>")
            response.toString
          case Failure(e) =>
            failedCalls.inc()
            log.info(s"Failed call: <$req, $e>")
            e.getClass.toString
        }
      }

    sc.run()
  }
}
