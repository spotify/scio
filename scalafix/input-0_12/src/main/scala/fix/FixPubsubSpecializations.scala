/*
rule = FixPubsubSpecializations
 */
package fix
package v0_12_0

import com.spotify.scio.ScioContext
import com.spotify.scio.pubsub._

object FixPubsubSpecializations {
  type MessageDummy = com.google.protobuf.DynamicMessage
  type PubSubMessageDummy = org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage

  class SpecificRecordDummy extends org.apache.avro.specific.SpecificRecordBase {
    override def getSchema = ???
    override def get(field: Int) = ???
    override def put(field: Int, value: Any) = ???
  }

  def contextPubsubSubscription(sc: ScioContext): Unit =
    sc.pubsubSubscription[String]("theName")

  def contextPubsubTopic(sc: ScioContext): Unit =
    sc.pubsubTopic[String]("theName")

  def contextPubsubSubscriptionWithAtt(sc: ScioContext): Unit =
    sc.pubsubSubscriptionWithAttributes[String]("theName")

  def contextPubsubTopicWithAtt(sc: ScioContext): Unit =
    sc.pubsubTopicWithAttributes[String]("theName")

  def contextPubsubSubscriptionSpecifiRecord(sc: ScioContext): Unit =
    sc.pubsubSubscription[SpecificRecordDummy]("theName")

  def contextPubsubTopicSpecifiRecord(sc: ScioContext): Unit =
    sc.pubsubTopic[SpecificRecordDummy]("theName")

  def contextPubsubSubscriptionWithAttSpecifiRecord(sc: ScioContext): Unit =
    sc.pubsubSubscriptionWithAttributes[SpecificRecordDummy]("theName")

  def contextPubsubTopicWithAttSpecifiRecord(sc: ScioContext): Unit =
    sc.pubsubTopicWithAttributes[SpecificRecordDummy]("theName")

  def pubsubApplySpecificRecord(): Unit =
    PubsubIO[SpecificRecordDummy]("theName")

  def pubsubApplySpecificRecordWithParams(): Unit =
    PubsubIO[SpecificRecordDummy]("theName", "idAtt", "timestampAtt")

  def pubsubApplySpecificRecordWithNamedParams(): Unit =
    PubsubIO[SpecificRecordDummy]("theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def pubsubApplyProto(): Unit =
    PubsubIO[MessageDummy]("theName")

  def pubsubApplyPubsub(): Unit =
    PubsubIO[PubSubMessageDummy]("theName")

  def pubsubApplyString(): Unit =
    PubsubIO[String]("theName")

  def pubsubApplyStringWithParams(): Unit =
    PubsubIO[String]("theName", "idAtt", "timestampAtt")

  def pubsubApplyStringWithNamedParams(): Unit =
    PubsubIO[String]("theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

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