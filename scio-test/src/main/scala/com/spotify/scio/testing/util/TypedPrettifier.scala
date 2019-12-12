package com.spotify.scio.testing.util
import com.spotify.scio.schemas.Schema
import org.scalactic.Prettifier

/**
 * A wrapper over Scalatic's [[Prettifier]] which allows us to override the behavior of
 * a specific type.
 *
 * Instances of `TypedPrettifier[T]` tries to pull in the `Schema[T]` and use the schema
 * for prettifying a Traversable[T].
 *
 * For cases where Schema derivation fallsback or we are unable to find and appropriate
 * schema we fall back and use the default scalactic's [[Prettifier]].
 *
 * Instances of `TypedPrettifier` are pulled in when a matcher is created using
 * `containsInAnyOrder`. To maintain backward compatibility for cases where a Schema is not
 * available we fallback using a Low Priority implicit to use the default scalactic Prettifier.
 *
 * Prettifiers are injected into the matcher and is used by scalatest to provide better String
 * representation of SCollections in error messages when an assertion fails in the test.
 */
trait TypedPrettifier[T] extends Serializable {

  /**
   * The scalatic prettifier wrapped for the given type T.
   */
  def apply: Prettifier
}

object TypedPrettifier extends TypedPrettifierInstances {

  def apply[T](t: T)(implicit typedPrettifier: TypedPrettifier[T]): String =
    typedPrettifier.apply(t)
}
