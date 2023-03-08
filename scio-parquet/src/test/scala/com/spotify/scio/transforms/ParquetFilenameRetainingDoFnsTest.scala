package com.spotify.scio.transforms

import com.spotify.scio.avro.AvroUtils

class ParquetFilenameRetainingDoFnsTest extends FilenameRetainingDoFnsSpec {

  it should "work with parquet-avro" in {
    withTempDir("filename-retaining-parquet") { temp =>
      import com.spotify.scio.parquet.avro._

      runWithRealContext(localOptions) { sc =>
        sc
          .parallelize(1 to 10)
          .map(AvroUtils.newSpecificRecord)
          .saveAsParquetAvroFile(temp.toString)
      }

      // specific records
      runWithRealContext(localOptions) { sc =>
        sc.parallelize(List(s"${temp.toString}/*.parquet"))
          .readFilesWithFilename(
          )
          .mapValues(_.getStrField)
          .debug()
      }
    }
  }
}
