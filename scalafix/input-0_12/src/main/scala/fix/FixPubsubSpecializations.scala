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

  def readAvro(): Unit =
    PubsubIO.readAvro[SpecificRecordDummy]("theName")

  def readAvroAllParams(): Unit =
    PubsubIO.readAvro[SpecificRecordDummy]("theName", "idAtt", "timestampAtt")

  def readAvroNamedParams(): Unit =
    PubsubIO.readAvro[SpecificRecordDummy](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readProto(): Unit =
    PubsubIO.readProto[MessageDummy]("theName")

  def readProtoAllParams(): Unit =
    PubsubIO.readProto[MessageDummy]("theName", "idAtt", "timestampAtt")

  def readProtoNamedParams(): Unit =
    PubsubIO.readProto[MessageDummy](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readPubsub(): Unit =
    PubsubIO.readPubsub[PubSubMessageDummy]("theName")

  def readPubsubAllParams(): Unit =
    PubsubIO.readPubsub[PubSubMessageDummy]("theName", "idAtt", "timestampAtt")

  def readPubsubNamedParams(): Unit =
    PubsubIO.readPubsub[PubSubMessageDummy](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readCoder(): Unit =
    PubsubIO.readCoder[String]("theName")

  def readCoderAllParams(): Unit =
    PubsubIO.readCoder[String]("theName", "idAtt", "timestampAtt")

  def readCoderNamedParams(): Unit =
    PubsubIO.readCoder[String](name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def readString(): Unit =
    PubsubIO.readString("theName")

  def readStringAllParams(): Unit =
    PubsubIO.readString("theName", "idAtt", "timestampAtt")

  def readStringNamedParams(): Unit =
    PubsubIO.readString(name = "theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")
}