import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{JsonProperties, Schema}
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO

import scala.jdk.CollectionConverters._

// TODO: Delete me before merge
object AvroData {
  lazy val UserDataSchema: Schema = Schema.createRecord(
    "UserData",
    "doc",
    "com.spotify.scio.examples.extra",
    false,
    List(
      new Field("userId", Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE),
      new Field("tag", Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE),
      new Field("size", Schema.create(Schema.Type.INT), "doc", JsonProperties.NULL_VALUE)
    ).asJava
  )

  def user(id: String, tag: String): GenericRecord = {
    val gr = new GenericData.Record(UserDataSchema)
    gr.put("userId", id)
    gr.put("tag", tag)
    gr.put("size", 0)
    gr
  }

  def user(aUser: GenericRecord, size: Int): GenericRecord = {
    val ngr = new GenericData.Record(UserDataSchema)
    ngr.put("userId", aUser.get("userId"))
    ngr.put("tag", aUser.get("tag"))
    ngr.put("size", size)
    ngr
  }
}
object SmbBiFunctionTest {

  import AvroData._
  import com.spotify.scio.smb._

  implicit val coder: Coder[GenericRecord] =
    Coder.avroGenericRecordCoder(AvroData.UserDataSchema)
  def main(cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdArgs)

    sc.parallelize(1 to 500)
      .map(a => (s"${a % 100}", s"${a}_tag"))
      .map { case (id, tag) =>
        user(id, tag)
      }
      .keyBy(_.get("userId"))
      .groupByKey
      .flatMap { case (userId, userDataList) =>
        userDataList.map(gr => user(gr, userDataList.size))
      }
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(classOf[String], "userId", UserDataSchema)
          .to(args("output"))
      )

    sc.run()
  }
}
