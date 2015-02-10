package com.spotify.cloud.dataflow.values

import com.spotify.cloud.dataflow.util.StatCounter

class DoubleSCollectionFunctions(self: SCollection[Double]) {

  def stats(): SCollection[StatCounter] = self.combine(StatCounter(_))(_.merge(_))(_.merge(_))

  // def mean(): SCollection[Double] = this.stats().map(_.mean)

  // def sum(): SCollection[Double] = this.stats().map(_.sum)

  def stdev(): SCollection[Double] = this.stats().map(_.stdev)

  def variance(): SCollection[Double] = this.stats().map(_.variance)

  def sampleStdev(): SCollection[Double] = this.stats().map(_.sampleStdev)

  def sampleVariance(): SCollection[Double] = this.stats().map(_.sampleVariance)

}
