package com.spotify.scio.avro.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroMagnolifyTyped
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import _root_.magnolify.avro.{AvroType => MagnolifyAvroType}

final class MagnolifyAvroScioContextOps(private val self: ScioContext) extends AnyVal {

  def typedAvroFile[T: MagnolifyAvroType: Coder](path: String): SCollection[T] =
    self.read(AvroMagnolifyTyped[T](path))(AvroMagnolifyTyped.ReadParam())

  def typedAvroFile[T: MagnolifyAvroType: Coder](path: String, suffix: String): SCollection[T] =
    self.read(AvroMagnolifyTyped[T](path))(AvroMagnolifyTyped.ReadParam(suffix))
}

trait MagnolifyAvroScioContextSyntax {
  implicit def magnolifyAvroScioContextOps(c: ScioContext): MagnolifyAvroScioContextOps =
    new MagnolifyAvroScioContextOps(c)
}
