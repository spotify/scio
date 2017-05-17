package com.spotify.scio.extra

import breeze.linalg.SparseVector
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Max

package object libsvm {

  implicit class SVMReader(@transient val self: ScioContext) extends Serializable {

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
        self.parallelize(List(numFeatures))
      } else {
        data.map { case (_, indices, _) =>
          indices.lastOption.getOrElse(0)
        }.sum(Max.maxSemigroup).map(_ + 1)
      }

      data.cross(featureCntCol).map { case ((label, indicies, values), featureCount) =>
        (label, SparseVector[Double](featureCount)(indicies.zip(values): _*))
      }
    }

    /**
      * Loads labeled data in the LIBSVM format into an SCollection[(Double, SparseVector)].
      * The LIBSVM format is a text-based format used by LIBSVM and LIBLINEAR.
      * Each line represents a labeled sparse feature vector using the following format:
      * {{{label index1:value1 index2:value2 ...}}}
      * where the indices are one-based and in ascending order.
      *
      * @param path        file or directory path in any Hadoop-supported file system URI
      * @param numFeatures number of features, which will be determined from the input data if a
      *                    nonpositive value is given. This is useful when the data is split
      *                    into multiple files and you want to load them separately, because some
      *                    features may not present in certain files, which leads to inconsistent
      *                    feature dimensions.
      * @return labeled data stored as an SCollection[(Double, SparseVector)]
      */
    def libSVMFile(path: String,
                   numFeatures: Int = 0)
    : SCollection[(Double, SparseVector[Double])] =
      libSVMCollection(self.textFile(path), numFeatures)
  }

}
