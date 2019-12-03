package com.spotify.scio.testing.util
import com.spotify.scio.schemas.Schema
import org.scalactic.Prettifier

/**
 * A wrapper over Scalatic's [[Prettifier]] which allows us to override the behavior of
 * a specific type.
 *
 * We use this to pull in the [[Schema]] of the type [[T]] so that the Schema can be used
 * to create a better representation of an `SCollection[T]` in the error messages.
 */
trait TypedPrettifier[T] extends Serializable {
  def apply: Prettifier
}

/**
 * A low priority fall back [[TypedPrettifier]] which delegates Scalactic's Prettifier.
 */
trait LowPriorityFallbackTypedPrettifier {

  /**
   * Visible when we fail to have a [[Schema]]
   */
  implicit def noSchemaPrettifier[T](
    implicit scalactic: Prettifier
  ): TypedPrettifier[T] =
    new TypedPrettifier[T] {
      override def apply: Prettifier = scalactic
    }
}

object TypedPrettifier extends LowPriorityFallbackTypedPrettifier {

  /**
   * An instance of [[TypedPrettifier]] when we have a [[Schema]] available
   * for our type. We use the Schema to create a table representation of
   * the SCollection[T]
   */
  implicit def schemaPrettifier[T: Schema](
    implicit schema: Schema[T],
    scalactic: Prettifier
  ): TypedPrettifier[T] =
    new TypedPrettifier[T] {
      override def apply: Prettifier =
        SCollectionPrettifier.getPrettifier[T](schema, scalactic)
    }
}
