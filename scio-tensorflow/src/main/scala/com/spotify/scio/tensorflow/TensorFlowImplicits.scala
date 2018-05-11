package com.spotify.scio.tensorflow

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.tensorflow.example.Example

import scala.reflect.ClassTag
import scala.language.implicitConversions

trait TensorFlowImplicits {

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[PredictSCollectionFunctions]].
   */
  implicit def makePredictSCollectionFunctions[T: ClassTag](
    s: SCollection[T]): PredictSCollectionFunctions[T] = new PredictSCollectionFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFRecordSCollectionFunctions]].
   */
  implicit def makeTFRecordSCollectionFunctions[T <: Array[Byte]](
    s: SCollection[T]): TFRecordSCollectionFunctions[T] = new TFRecordSCollectionFunctions(s)

  /** Implicit conversion from [[ScioContext]] to [[TFScioContextFunctions]]. */
  implicit def makeTFScioContextFunctions(s: ScioContext): TFScioContextFunctions =
    new TFScioContextFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFExampleSCollectionFunctions]].
   */
  implicit def makeTFExampleSCollectionFunctions[T <: Example](
    s: SCollection[T]): TFExampleSCollectionFunctions[T] = new TFExampleSCollectionFunctions(s)

  /**
   * Implicit conversion from [[com.spotify.scio.values.SCollection SCollection]] to
   * [[TFExampleSCollectionFunctions]].
   */
  implicit def makeSeqTFExampleSCollectionFunctions[T <: Example](
    s: SCollection[Seq[T]]): SeqTFExampleSCollectionFunctions[T] =
    new SeqTFExampleSCollectionFunctions(s)

}

object TensorFlowImplicits extends TensorFlowImplicits
