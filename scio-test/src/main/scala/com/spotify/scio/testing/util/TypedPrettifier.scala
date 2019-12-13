package com.spotify.scio.testing.util
import org.scalactic.Prettifier

/**
 * A wrapper over Scalatic's [[Prettifier]] which allows us to override the behavior of
 * a specific type.
 *
 * Instances of `TypedPrettifier` are available in [[TypedPrettifierInstances]]
 * and are resolved by implicit search.
 *
 * By default we have TypedPrettifiers for [[Traversable[IndexedRecord]]]
 *
 * Instances of `TypedPrettifier` are pulled in when a matcher is created using
 * `containsInAnyOrder`.
 *
 * Prettifiers are used to generate better error messages when an assertion fails in the test.
 */
trait TypedPrettifier[T] extends Serializable {

  def apply(t: T): String
}

object TypedPrettifier extends TypedPrettifierInstances {

  /**
   * Prettify a `Traversable[T]` in a Tabular form if a [[TypedPrettifier]] for [[T]]
   * is available.
   */
  def apply[T](t: Traversable[T])(
    implicit typedPrettifier: TypedPrettifier[Traversable[T]],
    dummyImplicit: DummyImplicit
  ): String =
    typedPrettifier(t)

  /**
   * Prettify a Type [[T]] based on current implicit scope.
   */
  def apply[T](t: T)(implicit typedPrettifier: TypedPrettifier[T]): String =
    typedPrettifier(t)
}
