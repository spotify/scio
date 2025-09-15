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
    sc.read(PubsubIO.string("theName"))(PubsubIO.ReadParam(PubsubIO.Subscription))

  def contextPubsubTopic(sc: ScioContext): Unit =
    sc.read(PubsubIO.string("theName"))(PubsubIO.ReadParam(PubsubIO.Topic))

  def contextPubsubSubscriptionWithAtt(sc: ScioContext): Unit =
    sc.read(PubsubIO.withAttributes[String]("theName"))(PubsubIO.ReadParam(PubsubIO.Subscription))

  def contextPubsubTopicWithAtt(sc: ScioContext): Unit =
    sc.read(PubsubIO.withAttributes[String]("theName"))(PubsubIO.ReadParam(PubsubIO.Topic))

  def contextPubsubSubscriptionSpecifiRecord(sc: ScioContext): Unit =
    sc.read(PubsubIO.avro[SpecificRecordDummy]("theName"))(PubsubIO.ReadParam(PubsubIO.Subscription))

  def contextPubsubTopicSpecifiRecord(sc: ScioContext): Unit =
    sc.read(PubsubIO.avro[SpecificRecordDummy]("theName"))(PubsubIO.ReadParam(PubsubIO.Topic))

  def contextPubsubSubscriptionWithAttSpecifiRecord(sc: ScioContext): Unit =
    sc.read(PubsubIO.withAttributes[SpecificRecordDummy]("theName"))(PubsubIO.ReadParam(PubsubIO.Subscription))

  def contextPubsubTopicWithAttSpecifiRecord(sc: ScioContext): Unit =
    sc.read(PubsubIO.withAttributes[SpecificRecordDummy]("theName"))(PubsubIO.ReadParam(PubsubIO.Topic))

  def pubsubApplySpecificRecord(): Unit =
    PubsubIO.avro[SpecificRecordDummy]("theName")

  def pubsubApplySpecificRecordWithParams(): Unit =
    PubsubIO.avro[SpecificRecordDummy]("theName", "idAtt", "timestampAtt")

  def pubsubApplySpecificRecordWithNamedParams(): Unit =
    PubsubIO.avro[SpecificRecordDummy]("theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

  def pubsubApplyProto(): Unit =
    PubsubIO.proto[MessageDummy]("theName")

  def pubsubApplyPubsub(): Unit =
    PubsubIO.pubsub[PubSubMessageDummy]("theName")

  def pubsubApplyString(): Unit =
    PubsubIO.string("theName")

  def pubsubApplyStringWithParams(): Unit =
    PubsubIO.string("theName", "idAtt", "timestampAtt")

  def pubsubApplyStringWithNamedParams(): Unit =
    PubsubIO.string("theName", timestampAttribute = "idAtt", idAttribute = "timestampAtt")

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

  def writePubSub(scoll: SCollection[String]): Unit = {
    val pubsubProjectId = "pubsubProjectId"
    scoll.write(PubsubIO.string("projects/" + pubsubProjectId + "/topics/xxx"))(PubsubIO.WriteParam())
  }

  def writePubSubAllArgs(scoll: SCollection[SpecificRecordDummy]): Unit = {
    scoll.write(PubsubIO.avro[SpecificRecordDummy]("Topic", "TdAttribute", "TimestampAttribute"))(PubsubIO.WriteParam(Some(1), Some(2)))
  }

  def writePubSubSomeArgsNamed(scoll: SCollection[SpecificRecordDummy]): Unit = {
    scoll.write(PubsubIO.avro[SpecificRecordDummy]("topic", "IdAttribute", timestampAttribute = "TimestampAttribute"))(PubsubIO.WriteParam(maxBatchBytesSize = Some(1)))
  }

  def writePubSubWithAttributes(scoll: SCollection[(String, Map[String, String])]): Unit = {
    scoll.write(PubsubIO.withAttributes[String]("topic"))(PubsubIO.WriteParam())
  }

  def writePubSubWithAttributesTyped(scoll: SCollection[(MessageDummy, Map[String, String])]): Unit = {
    scoll.write(PubsubIO.withAttributes[MessageDummy]("topic"))(PubsubIO.WriteParam())
  }

  def writePubSubWithAttributesTypedWithAllArgs(scoll: SCollection[(MessageDummy, Map[String, String])]): Unit = {
    scoll.write(PubsubIO.withAttributes[MessageDummy]("topic", "IdAttribute", "TimestampAttribute"))(PubsubIO.WriteParam(Some(1), Some(2)))
  }

  def writePubSubWithAttributesTypedWithSomeArgsNamed(scoll: SCollection[(PubSubMessageDummy, Map[String, String])]): Unit = {
    scoll.write(PubsubIO.withAttributes[PubSubMessageDummy]("topic", "IdAttribute", timestampAttribute = "TimestampAttribute"))(PubsubIO.WriteParam(maxBatchBytesSize = Some(1)))
  }
}