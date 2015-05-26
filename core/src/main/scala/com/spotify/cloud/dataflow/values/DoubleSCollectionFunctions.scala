package com.spotify.cloud.dataflow.values

import com.spotify.cloud.dataflow.util.StatCounter

/**
 * Extra functions available on SCollections of Doubles through an implicit conversion.
 */
class DoubleSCollectionFunctions(self: SCollection[Double]) {

  /**
   * Return an SCollection with a single [[com.spotify.cloud.dataflow.util.StatCounter]] object
   * that captures the mean, variance and count of the SCollection's elements in one operation.
   */
  def stats(): SCollection[StatCounter] = self.combine(StatCounter(_))(_.merge(_))(_.merge(_))

  // Implemented in SCollection
  // def mean(): SCollection[Double] = this.stats().map(_.mean)

  // Implemented in SCollection
  // def sum(): SCollection[Double] = this.stats().map(_.sum)

  /** Compute the standard deviation of this SCollection's elements. */
  def stdev(): SCollection[Double] = this.stats().map(_.stdev)

  /** Compute the variance of this SCollection's elements. */
  def variance(): SCollection[Double] = this.stats().map(_.variance)

  /**
   * Compute the sample standard deviation of this SCollection's elements (which corrects for bias
   * in estimating the standard deviation by dividing by N-1 instead of N).
   */
  def sampleStdev(): SCollection[Double] = this.stats().map(_.sampleStdev)

  /**
   * Compute the sample variance of this SCollection's elements (which corrects for bias in
   * estimating the variance by dividing by N-1 instead of N).
   */
  def sampleVariance(): SCollection[Double] = this.stats().map(_.sampleVariance)

  // TODO: implement histogram

}
