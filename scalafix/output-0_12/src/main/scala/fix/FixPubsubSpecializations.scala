package fix
package v0_12_0

import com.spotify.scio.pubsub.PubsubIO

object FixPubsubSpecializations {
  type MessageDummy = com.google.protobuf.DynamicMessage
  type SpecificRecordDummy = org.apache.avro.specific.SpecificRecordBase
  type PubSubMessageDummy = org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

  def avro(): Unit =
    PubsubIO.avro[SpecificRecordDummy]("theName")

  def proto(): Unit =
    PubsubIO.proto[MessageDummy]("theName")

  def pubsub(): Unit =
    PubsubIO.pubsub[PubSubMessageDummy]("theName")

  def coder(): Unit =
    PubsubIO.coder[String]("theName")

  def readString(): Unit =
    PubsubIO.string("theName")

  def readStringAllParams(): Unit =
    PubsubIO.string("theName", "idAtt", "timestampAtt")

  def readStringNamedParams(): Unit =
    PubsubIO.string(name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")
}