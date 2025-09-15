/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.examples.extra

/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.spotify.scio.avro.Account
import com.spotify.scio.examples.extra.ParquetExample.{AccountFull, AccountProjection}
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.types._
import com.spotify.scio.parquet.tensorflow._
import com.spotify.scio.io.TextIO
import org.tensorflow.proto.example.{Example, Feature, Features, FloatList}

class ParquetExampleTest extends PipelineSpec {

  "ParquetExample" should "work for SpecificRecord input" in {
    val expected = ParquetExample.fakeData
      .map(x => AccountProjection(x.getId, Some(x.getName.toString)))
      .map(_.toString)

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--input=in.parquet", "--output=out.txt", "--method=avroSpecificIn")
      .input(ParquetAvroIO[Account]("in.parquet"), ParquetExample.fakeData)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(expected))
      .run()
  }

  it should "work for typed input" in {
    val input = ParquetExample.fakeData
      .map(x => AccountProjection(x.getId, Some(x.getName.toString)))

    val expected = input.map(_.toString)

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--input=in.parquet", "--output=out.txt", "--method=typedIn")
      .input(ParquetTypeIO[AccountProjection]("in.parquet"), input)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(expected))
      .run()
  }

  it should "work for SpecificRecord output" in {
    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--output=out.parquet", "--method=avroOut")
      .output(ParquetAvroIO[Account]("out.parquet"))(coll =>
        coll should containInAnyOrder(ParquetExample.fakeData)
      )
      .run()
  }

  it should "work for typed output" in {
    val expected = ParquetExample.fakeData
      .map(a => AccountFull(a.getId, a.getType.toString, Some(a.getName.toString), a.getAmount))

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--output=out.parquet", "--method=typedOut")
      .output(ParquetTypeIO[AccountFull]("out.parquet"))(coll =>
        coll should containInAnyOrder(expected)
      )
      .run()
  }

  it should "work for Example output" in {
    val expected = ParquetExample.fakeData.map(ParquetExample.toExample)

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--output=example-out", "--method=exampleOut")
      .output(ParquetExampleIO("example-out"))(coll => coll should containInAnyOrder(expected))
      .run()
  }

  it should "work for Example input" in {
    val input = ParquetExample.fakeData.map(ParquetExample.toExample)
    val projectedOutput = ParquetExample.fakeData
      .map { account =>
        val features = Features
          .newBuilder()
          .putFeature(
            "amount",
            Feature
              .newBuilder()
              .setFloatList(FloatList.newBuilder().addValue(account.getAmount.toFloat))
              .build()
          )

        Example.newBuilder().setFeatures(features).build()
      }
      .map(_.toString)

    JobTest[com.spotify.scio.examples.extra.ParquetExample.type]
      .args("--input=example-in", "--method=exampleIn", "--output=example-out")
      .input(ParquetExampleIO("example-in"), input)
      .output(TextIO("example-out"))(coll => coll should containInAnyOrder(projectedOutput))
      .run()
  }
}
