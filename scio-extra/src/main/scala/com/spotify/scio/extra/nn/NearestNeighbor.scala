/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra.nn

import breeze.linalg._
import breeze.math._
import com.google.common.collect.MinMaxPriorityQueue
import info.debatty.java.lsh.LSHSuperBit

import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer => MBuffer, Map => MMap, Set => MSet}
import scala.reflect.ClassTag
import scala.{specialized => sp}

/** Utilities for creating [[NearestNeighborBuilder]] instances. */
object NearestNeighbor {

  /**
   * Create a new builder for LSH based [[NearestNeighbor]].
   * @param dimension dimension of input vectors
   * @param stages number of times a vector is bucketed
   * @param buckets number of buckets per stage
   */
  def newLSHBuilder[K: ClassTag, @sp(Double, Int, Float, Long) V: ClassTag : Numeric : Semiring]
  (dimension: Int, stages: Int, buckets: Int): NearestNeighborBuilder[K, V] =
    new LSHNNBuilder[K, V](dimension, stages, buckets)

  /**
   * Create a new builder for matrix based [[NearestNeighbor]].
   * @param dimension dimension of input vectors
   */
  def newMatrixBuilder[K: ClassTag, @sp(Double, Int, Float, Long) V: ClassTag : Numeric : Semiring]
  (dimension: Int): NearestNeighborBuilder[K, V] =
    new MatrixNNBuilder[K, V](dimension)

}

/** Builder for immutable [[NearestNeighbor]] instances. */
trait NearestNeighborBuilder[K, @sp(Double, Int, Float, Long) V] extends Serializable {

  /** Dimension of item vectors. */
  protected val dimension: Int

  /** Item key to numeric id mapping. */
  protected val keyToId = MMap.empty[K, Int]

  /** Numeric id to item key mapping. */
  protected val idToKey = MBuffer.empty[K]

  /** Raw item vectors. */
  protected val vectors = MBuffer.empty[DenseVector[V]]

  /** Add a key->vector pair to common storage. */
  protected def addVector(key: K, vec: DenseVector[V]): Int = {
    require(vec.length == dimension, s"Vector dimension ${vec.length} != $dimension")
    require(!keyToId.contains(key), s"Key $key already exists")

    val id = keyToId.size
    keyToId(key) = id
    idToKey.append(key)
    vectors.append(vec)

    id
  }

  /** Add a key->vector pair. The vector should be normalized. */
  def add(key: K, vec: DenseVector[V]): Unit

  /** Build an immutable NearestNeighbor instance. */
  def build: NearestNeighbor[K, V]

}

/**
 * Immutable index for fast nearest neighbor look up.
 *
 * {{
 * import com.spotify.scio.extra.nn._
 *
 * // Tuples of (item ID, vector) to look up from
 * val vectors: Seq[(String, DenseVector[Double])] = // ...
 *
 * // Builder for a nearest neighbor index backed by a matrix
 * val builder = NearestNeighbor.newMatrixBuilder[String, Double](40)
 * vectors.foreach(kv => builder.addVector(kv._1, kv._2))
 * val nn = builder.build
 *
 * // Candidate to loo up nearest neighbors from
 * val candidate: DenseVector[Double] = // ...
 *
 * // Look up top 10 most similar items
 * nn.lookup(candidate, 10)
 * }}
 */
trait NearestNeighbor[K, @sp(Double, Int, Float, Long) V] extends Serializable {

  /** Name of the nearest neighbor method. */
  val name: String

  /** Dimension of item vectors. */
  protected val dimension: Int

  /** Item key to numeric id mapping. */
  protected val keyToId: Map[K, Int]

  /** Numeric id to item key mapping. */
  protected val idToKey: Array[K]

  /** Raw item vectors. */
  protected val vectors: Array[DenseVector[V]]

  @inline protected def getKey(id: Int): K = idToKey(id)
  @inline protected def getId(key: K): Int = keyToId(key)

  /** Lookup nearest neighbors of a vector. The vector should be normalized. */
  def lookup(vec: DenseVector[V], maxResult: Int,
             minSimilarity: Double = Double.NegativeInfinity): Iterable[(K, Double)]

  /** Lookup nearest neighbors of an existing vector. */
  def lookupKey(key: K, maxResult: Int,
                minSimilarity: Double = Double.NegativeInfinity): Iterable[(K, Double)] =
    lookup(vectors(getId(key)), maxResult, minSimilarity)

}

/** Builder for [[MatrixNN]]. */
private class
MatrixNNBuilder[K: ClassTag, @sp(Double, Int, Float, Long) V: ClassTag : Numeric : Semiring]
  (override val dimension: Int)
  extends NearestNeighborBuilder[K, V] {

  /** Add a key->vector pair. The vector should be normalized. */
  override def add(key: K, vec: DenseVector[V]): Unit = addVector(key, vec)

  /** Build an immutable NearestNeighbor instance. */
  override def build: NearestNeighbor[K, V] =
    new MatrixNN(
      dimension, keyToId.toMap, idToKey.toArray, vectors.toArray,
      DenseMatrix(vectors.map(_.toArray): _*))
}

/** Nearest neighbor using vector dot product via matrix multiplication. */
private class MatrixNN[K, @sp(Double, Int, Float, Long) V: ClassTag : Numeric : Semiring]
  (override protected val dimension: Int,
   override protected val keyToId: Map[K, Int],
   override protected val idToKey: Array[K],
   override val vectors: Array[DenseVector[V]],
   private val matrix: Matrix[V])
  extends NearestNeighbor[K, V] {

  /** Name of the nearest neighbor method. */
  override val name: String = "Matrix"

  /** Lookup nearest neighbors of a vector. The vector should be normalized. */
  override def lookup(vec: DenseVector[V], maxResult: Int,
                      minSimilarity: Double): Iterable[(K, Double)] = {
    require(vec.length == dimension, s"Vector dimension ${vec.length} != $dimension")
    require(maxResult > 0, s"maxResult must be > 0")

    val sim = matrix * vec

    val pq = MinMaxPriorityQueue.orderedBy[(Int, Double)](Ordering.by(-_._2))
      .expectedSize(maxResult)
      .maximumSize(maxResult)
      .create[(Int, Double)]()

    val numeric = implicitly[Numeric[V]]
    var i = 0
    while (i < idToKey.length) {
      val cosine = numeric.toDouble(sim(i))
      if (cosine >= minSimilarity) {
        pq.add((i, cosine))
      }
      i += 1
    }
    pq.asScala.map { case (id, v) => (getKey(id), v) }
  }

}

/** Builder for [[LSHNN]]. */
private class
LSHNNBuilder[K: ClassTag, @sp(Double, Int, Float, Long) V: ClassTag : Numeric : Semiring]
  (override val dimension: Int, val stages: Int, val buckets: Int)
  extends NearestNeighborBuilder[K, V] {

  require(stages > 0, "stages must be > 0")
  require(buckets > 0, "buckets must be > 0")
  require(dimension > 0, "dimension must be > 0")

  private val lsh = new LSHSuperBit(stages, buckets, dimension)
  private val bins = Array.fill(buckets)(MBuffer.empty[Int])

  /** Add a key->vector pair. The vector should be normalized. */
  override def add(key: K, vec: DenseVector[V]): Unit = {
    val id = addVector(key, vec)

    val numeric = implicitly[Numeric[V]]
    val buckets = lsh.hash(vec.toArray.map(numeric.toDouble))
    var i = 0
    while (i < buckets.length) {
      bins(buckets(i)).append(id)
      i += 1
    }
  }

  /** Build an immutable NearestNeighbor instance. */
  override def build: NearestNeighbor[K, V] =
    new LSHNN(dimension, keyToId.toMap, idToKey.toArray, vectors.toArray, lsh, bins.map(_.toArray))

}

/** Nearest neighbor using Locality Sensitive Hashing. */
private class LSHNN[K, @sp(Double, Int, Float, Long) V: ClassTag : Numeric : Semiring]
  (override protected val dimension: Int,
   override protected val keyToId: Map[K, Int],
   override protected val idToKey: Array[K],
   override val vectors: Array[DenseVector[V]],
   private val lsh: LSHSuperBit,
   private val bins: Array[Array[Int]])
  extends NearestNeighbor[K, V] {

  /** Name of the nearest neighbor method. */
  override val name: String = "LSH"

  /** Lookup nearest neighbors of a vector. The vector should be normalized. */
  override def lookup(vec: DenseVector[V], maxResult: Int,
                      minSimilarity: Double): Iterable[(K, Double)] = {
    require(vec.length == dimension, s"Vector dimension ${vec.length} != $dimension")
    require(maxResult > 0, s"maxResult must be > 0")

    val numeric = implicitly[Numeric[V]]
    val buckets = lsh.hash(vec.toArray.map(numeric.toDouble))

    val pq = MinMaxPriorityQueue.orderedBy[(Int, Double)](Ordering.by(-_._2))
      .expectedSize(maxResult)
      .maximumSize(maxResult)
      .create[(Int, Double)]()

    var i = 0
    val set = MSet.empty[Int]
    while (i < buckets.length) {
      val b = bins(buckets(i))
      var j = 0
      while (j < b.length) {
        val id = b(j)
        if (!set.contains(id)) {
          set.add(id)
          val cosine = numeric.toDouble(vec dot vectors(id))
          if (cosine >= minSimilarity) {
            pq.add((id, cosine))
          }
        }
        j += 1
      }
      i += 1
    }
    pq.asScala.map { case (id, v) => (getKey(id), v) }
  }

}
