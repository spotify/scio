package com.spotify.scio.bigtable

import com.spotify.scio.coders.Coder
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.io.hbase.HBaseCoderProviderRegistrar
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.hbase.client.{Mutation, Result}

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait BigtableCoders {

  // FIXME hbase coders are protected. This access is risky
  private val mutationProvider :: _ :: resultProvider :: Nil =
    new HBaseCoderProviderRegistrar().getCoderProviders.asScala.toList

  implicit def mutationCoder[T <: Mutation: ClassTag]: Coder[T] = {
    val td = TypeDescriptor.of(ScioUtil.classOf[T])
    Coder.beam(mutationProvider.coderFor(td, Collections.emptyList()))
  }

  implicit val resultCoder: Coder[Result] =
    Coder.beam(resultProvider.coderFor(TypeDescriptor.of(classOf[Result]), Collections.emptyList()))

}

object BigtableCoders extends BigtableCoders
