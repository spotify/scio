package com.spotify.scio.io

import java.util.UUID

import org.apache.commons.io.FileUtils

class TFTapTest extends TapSpec {

  "SCollection" should "support saveAsTFRecordFile" in {
    import com.spotify.scio.tensorflow._
    val data = Seq.fill(100)(UUID.randomUUID().toString)
    import TFRecordOptions.CompressionType._
    for (compressionType <- Seq(NONE, ZLIB, GZIP)) {
      val dir = tmpDir
      val t = runWithFileFuture {
        _
          .parallelize(data)
          .map(_.getBytes)
          .saveAsTfRecordFile(dir.getPath, tfRecordOptions = TFRecordOptions(compressionType))
      }
      verifyTap(t.map(new String(_)), data.toSet)
      FileUtils.deleteDirectory(dir)
    }
  }

}
