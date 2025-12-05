package fix.v0_15_0

import org.tensorflow.proto.Example
import org.tensorflow.proto.Feature
import org.tensorflow.proto.Features

object FixTensorflowProtoImportsExample {
  def process(example: Example): Features = {
    example.getFeatures
  }

  def createFeature(): Feature = {
    Feature.newBuilder().build()
  }
}
