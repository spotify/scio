package fix
package v0_12_0

import com.spotify.scio.pubsub.PubsubIO

object FixPubsubSpecializations {
  type MessageDummy = com.google.protobuf.DynamicMessage
  type SpecificRecordDummy = org.apache.avro.specific.SpecificRecordBase
  type PubSubMessageDummy = org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

  def readAvro(): Unit =
    PubsubIO.avro[SpecificRecordDummy]("theName")

  def readAvroAllParams(): Unit =
    PubsubIO.avro[SpecificRecordDummy]("theName", "idAtt", "timestampAtt")

  def readAvroNamedParams(): Unit =
    PubsubIO.avro[SpecificRecordDummy](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readProto(): Unit =
    PubsubIO.proto[MessageDummy]("theName")

  def readProtoAllParams(): Unit =
    PubsubIO.proto[MessageDummy]("theName", "idAtt", "timestampAtt")

  def readProtoNamedParams(): Unit =
    PubsubIO.proto[MessageDummy](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readPubsub(): Unit =
    PubsubIO.pubsub[PubSubMessageDummy]("theName")

  def readPubsubAllParams(): Unit =
    PubsubIO.pubsub[PubSubMessageDummy]("theName", "idAtt", "timestampAtt")

  def readPubsubNamedParams(): Unit =
    PubsubIO.pubsub[PubSubMessageDummy](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readCoder(): Unit =
    PubsubIO.coder[String]("theName")

  def readCoderAllParams(): Unit =
    PubsubIO.coder[String]("theName", "idAtt", "timestampAtt")

  def readCoderNamedParams(): Unit =
    PubsubIO.coder[String](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readString(): Unit =
    PubsubIO.string("theName")

  def readStringAllParams(): Unit =
    PubsubIO.string("theName", "idAtt", "timestampAtt")

  def readStringNamedParams(): Unit =
    PubsubIO.string(name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")
}