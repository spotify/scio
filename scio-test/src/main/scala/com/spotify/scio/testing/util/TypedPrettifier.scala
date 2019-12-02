package com.spotify.scio.testing.util
import com.spotify.scio.schemas.Schema
import org.scalactic.Prettifier
import shapeless.Refute

trait TypedPrettifier[T] extends Serializable {
  def apply: Prettifier
}

object TypedPrettifier {
  implicit def noSchemaPrettifier[T](
    implicit scalactic: Prettifier,
    refute: Refute[Schema[T]]
  ): TypedPrettifier[T] =
    new TypedPrettifier[T] {
      override def apply: Prettifier = scalactic
    }

  implicit def schemaPrettifier[T: Schema](
    implicit schema: Schema[T],
    scalactic: Prettifier
  ): TypedPrettifier[T] =
    new TypedPrettifier[T] {
      override def apply: Prettifier =
        SCollectionPrettifier.getPrettifier[T](schema, scalactic)
    }
}
