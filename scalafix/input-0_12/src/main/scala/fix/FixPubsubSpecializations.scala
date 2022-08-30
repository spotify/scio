/*
rule = FixPubsubSpecializations
*/
package fix
package v0_12_0

import com.spotify.scio.pubsub.PubsubIO

object FixPubsubSpecializations {
  type MessageDummy = com.google.protobuf.DynamicMessage
  type SpecificRecordDummy = org.apache.avro.specific.SpecificRecordBase
  type PubSubMessageDummy = org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

  def avro(): Unit =
    PubsubIO.readAvro[SpecificRecordDummy]("theName")

  def proto(): Unit =
    PubsubIO.readProto[MessageDummy]("theName")

  def pubsub(): Unit =
    PubsubIO.readPubsub[PubSubMessageDummy]("theName")

  def coder(): Unit =
    PubsubIO.readCoder[String]("theName")

  def readString(): Unit =
    PubsubIO.readString("theName")

  def readStringAllParams(): Unit =
    PubsubIO.readString("theName", "idAtt", "timestampAtt")

  def readStringNamedParams(): Unit =
    PubsubIO.readString(name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")
}