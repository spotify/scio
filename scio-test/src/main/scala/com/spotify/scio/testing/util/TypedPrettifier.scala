package com.spotify.scio.testing.util
import com.spotify.scio.schemas.Schema
import org.apache.avro.generic.IndexedRecord
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

/**
 * A low priority fall back [[TypedPrettifier]] which delegates Scalactic's Prettifier.
 */
trait LowPriorityFallbackTypedPrettifier {
  /**
   * Visible when we fail to have a [[Schema]]
   *
   * This `TypedPrettifier` fallsback to the default scalactic Prettifier.
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
   * An instance of TypedPrettifier when we have an AvroRecord.
   * We use the Avro Schema to create an table representation of
   * the SCollection[T]
   */
  implicit def avroPrettifier[T <: IndexedRecord](
    implicit scalacticFallback: Prettifier
  ): TypedPrettifier[T] = {
    new TypedPrettifier[T] {
      override def apply: Prettifier =
        SCollectionPrettifier.getAvroRecordPrettifier(scalacticFallback)
    }
  }

  /*
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
 */
}
