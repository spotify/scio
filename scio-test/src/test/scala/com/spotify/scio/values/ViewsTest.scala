/*
 * Copyright 2020 Spotify AB.
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
package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollectionView
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection

final class DoFnWithSideInput[T, U](pv: PCollectionView[U]) extends DoFn[T, (T, U)] {
  @ProcessElement
  def processElement(c: DoFn[T, (T, U)]#ProcessContext): Unit = {
    c.output((c.element(), c.sideInput(pv)))
    ()
  }
}

final class ViewsTest extends PipelineSpec {

  import com.spotify.scio.coders._

  test[Int, List[Int]](View.asScalaList)(Seq(1), Seq((1, List(1))))
  test[Int, Iterable[Int]](View.asScalaIterable)(Seq(1), Seq((1, Iterable(1))))
  test[(String, Int), Map[String, Int]](View.asScalaMap)(
    Seq(("a", 1)),
    Seq((("a", 1), Map("a" -> 1)))
  )
  test[(String, Int), Map[String, Iterable[Int]]](View.asScalaMultimap)(
    Seq(("a", 1)),
    Seq((("a", 1), Map("a" -> Iterable(1))))
  )

  private def test[T: Coder, U: Coder](view: PTransform[PCollection[T], PCollectionView[U]])(
    data: Seq[T],
    expected: Seq[(T, U)]
  ) =
    it should s"support ${view.getClass().getSimpleName()}" in {
      runWithContext { sc =>
        val pv = sc.parallelize(data).applyInternal(view)
        val result = sc
          .wrap(
            sc.parallelize(data)
              .applyInternal(
                ParDo.of(new DoFnWithSideInput[T, U](pv)).withSideInputs(pv)
              )
          )
          .setCoder(CoderMaterializer.beam(sc, Coder[(T, U)]))

        result should containInAnyOrder(expected)
      }
    }
}
