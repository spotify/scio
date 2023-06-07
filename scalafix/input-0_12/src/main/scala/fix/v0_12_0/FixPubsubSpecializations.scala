/*
rule = FixPubsubSpecializations
 */
package fix.v0_12_0

import com.spotify.scio.ScioContext
import com.spotify.scio.pubsub._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord

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

  def writePubSub(scoll: SCollection[String]): Unit = {
    val pubsubProjectId = "pubsubProjectId"
    scoll.saveAsPubsub("projects/" + pubsubProjectId + "/topics/xxx")
  }

  def writePubSubAllArgs(scoll: SCollection[SpecificRecordDummy]): Unit = {
    scoll.saveAsPubsub("Topic", "TdAttribute", "TimestampAttribute", Some(1), Some(2))
  }

  def writePubSubSomeArgsNamed(scoll: SCollection[SpecificRecordDummy]): Unit = {
    scoll.saveAsPubsub("topic", "IdAttribute", maxBatchBytesSize = Some(1), timestampAttribute = "TimestampAttribute")
  }

  def writePubSubWithAttributes(scoll: SCollection[(String, Map[String, String])]): Unit = {
    scoll.saveAsPubsubWithAttributes[String]("topic")
  }

  def writePubSubWithAttributesTyped(scoll: SCollection[(MessageDummy, Map[String, String])]): Unit = {
    scoll.saveAsPubsubWithAttributes[MessageDummy]("topic")
  }

  def writePubSubWithAttributesTypedWithAllArgs(scoll: SCollection[(MessageDummy, Map[String, String])]): Unit = {
    scoll.saveAsPubsubWithAttributes[MessageDummy]("topic", "IdAttribute", "TimestampAttribute", Some(1), Some(2))
  }

  def writePubSubWithAttributesTypedWithSomeArgsNamed(scoll: SCollection[(PubSubMessageDummy, Map[String, String])]): Unit = {
    scoll.saveAsPubsubWithAttributes[PubSubMessageDummy]("topic", "IdAttribute", maxBatchBytesSize = Some(1), timestampAttribute = "TimestampAttribute")
  }
}