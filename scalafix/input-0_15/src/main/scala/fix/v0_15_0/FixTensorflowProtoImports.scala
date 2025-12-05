/*
rule = FixTensorflowProtoImports
*/
package fix.v0_15_0

import org.tensorflow.proto.example.Example
import org.tensorflow.proto.example.Feature
import org.tensorflow.proto.example.Features

object FixTensorflowProtoImportsExample {
  def process(example: Example): Features = {
    example.getFeatures
  }

  def createFeature(): Feature = {
    Feature.newBuilder().build()
  }
}
