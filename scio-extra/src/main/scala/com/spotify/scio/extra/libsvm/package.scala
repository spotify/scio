/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra

import breeze.linalg.SparseVector
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder

import com.spotify.scio.values.SCollection
import com.twitter.algebird.Max

/**
 * Main package for reading the Lib SVM Format
 *
 *
 * {{{
 * import com.spotify.scio.extra.libsvm._
 *
 * // Read SVM Lib as Label, SparseVector
 * sc.libSVMFile("input.svm")
 * }}}
 */
package object libsvm {

  private def parseLibSVMRecord(line: String): (Double, Array[Int], Array[Double]) = {
    val items = line.split(' ')
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1
      // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, s"indices should be one-based and in ascending order;"
        +
        s""" found current=$current, previous=$previous; line="$line"""")
      previous = current
      i += 1
    }
    (label, indices, values)
  }

  def libSVMCollection(col: SCollection[String],
                       numFeatures: Int = 0)
  : SCollection[(Double, SparseVector[Double])] = {
    val data = col
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLibSVMRecord)

    val featureCntCol = if (numFeatures > 0) {
      col.context.parallelize(List(numFeatures))
    } else {
      data.map { case (_, indices, _) =>
        indices.lastOption.getOrElse(0)
      }.sum(Max.maxSemigroup, Coder[Int]).map(_ + 1)
    }

    data.cross(featureCntCol)
      .map { case ((label, indicies, values), featureCount) =>
        (label, SparseVector[Double](featureCount)(indicies.zip(values): _*))
      }
  }

  implicit class SVMReader(@transient val self: ScioContext) extends Serializable {

    /**
     * Loads labeled data in the LIBSVM format into an SCollection[(Double, SparseVector)].
     * The LIBSVM format is a text-based format used by LIBSVM and LIBLINEAR.
     * Each line represents a labeled sparse feature vector using the following format:
     * [label index1:value1 index2:value2 ...]
     * where the indices are one-based and in ascending order.
     *
     * @param path        file or directory path in any Hadoop-supported file system URI
     * @param numFeatures number of features, which will be determined from the input data if a
     *                    nonpositive value is given. This is useful when the data is split
     *                    into multiple files and you want to load them separately, because some
     *                    features may not present in certain files, which leads to inconsistent
     *                    feature dimensions.
     * @return            labeled data stored as an SCollection[(Double, SparseVector)]
     */
    def libSVMFile(path: String,
                   numFeatures: Int = 0)
    : SCollection[(Double, SparseVector[Double])] =
      libSVMCollection(self.textFile(path), numFeatures)
  }

}
