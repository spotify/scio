package com.spotify.scio.extra.sparkey.instances

import com.spotify.sparkey.SparkeyReader

trait SparkeyReaderInstances {

  implicit def stringSparkeyReader(reader: SparkeyReader): StringSparkeyReader =
    new StringSparkeyReader(reader)

}
