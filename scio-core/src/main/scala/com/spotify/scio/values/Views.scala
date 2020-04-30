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

import java.{util => ju}

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.Materialization
import org.apache.beam.sdk.transforms.Materializations
import org.apache.beam.sdk.transforms.Materializations.MultimapView
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.View.CreatePCollectionView
import org.apache.beam.sdk.transforms.ViewFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn
import org.apache.beam.sdk.values.{PValue, TupleTag}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.PCollectionViews.TypeDescriptorSupplier
import org.apache.beam.sdk.values.PValueBase
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeParameter
import org.apache.beam.sdk.values.WindowingStrategy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util._

final private class ViewImpl[T, U, V, W <: BoundedWindow](
  @transient coll: PCollection[KV[Unit, T]],
  viewFn: ViewFn[U, V],
  windowMappingFn: WindowMappingFn[W],
  windowStrategy: WindowingStrategy[_, W]
) extends PValueBase(coll.getPipeline())
    with PCollectionView[V] {
  private[this] val coder = coll.getCoder()
  private[this] val tag = new TupleTag

  override def expand(): ju.Map[TupleTag[_], PValue] =
    ju.Collections.singletonMap(getTagInternal(), coll)

  override def getPCollection(): PCollection[_] = coll

  override def getTagInternal(): TupleTag[_] = tag

  override def getViewFn(): ViewFn[U, V] = viewFn

  override def getWindowMappingFn(): WindowMappingFn[_] = windowMappingFn

  override def getWindowingStrategyInternal(): WindowingStrategy[_, _] = windowStrategy

  override def getCoderInternal(): BCoder[_] = coder

  override def hashCode(): Int = ju.Objects.hash(tag)

  override def equals(obj: Any): Boolean = obj match {
    case _: PCollectionView[V] =>
      tag.equals(obj.asInstanceOf[PCollectionView[T]].getTagInternal())
    case _ =>
      false
  }
}

private object ViewImpl {
  final def apply[T, U, V, W <: BoundedWindow](
    coll: PCollection[KV[Unit, T]],
    viewFn: ViewFn[U, V],
    windowStrategy: WindowingStrategy[_, W]
  ): ViewImpl[T, U, V, W] =
    new ViewImpl(
      coll,
      viewFn,
      windowStrategy.getWindowFn().getDefaultWindowMappingFn(),
      windowStrategy
    )
}

final private class ScalaMultimapViewFn[K, V](
  keyTypeDescriptorSupplier: TypeDescriptorSupplier[K],
  valueTypeDescriptorSupplier: TypeDescriptorSupplier[V]
) extends ViewFn[MultimapView[Unit, (K, V)], Map[K, Iterable[V]]] {
  override def getMaterialization(): Materialization[MultimapView[Unit, (K, V)]] =
    Materializations.multimap()

  override def apply(primitiveViewT: MultimapView[Unit, (K, V)]): Map[K, Iterable[V]] = {
    val m = mutable.Map.empty[K, mutable.Builder[V, Iterable[V]]]
    for (elem <- primitiveViewT.get(()).iterator().asScala) {
      val bldr = m.getOrElseUpdate(elem._1, Iterable.newBuilder)
      bldr += elem._2
    }

    val b = Map.newBuilder[K, Iterable[V]]
    for ((k, v) <- m)
      b += ((k, v.result))

    b.result
  }

  override def getTypeDescriptor(): TypeDescriptor[Map[K, Iterable[V]]] =
    new TypeDescriptor[Map[K, Iterable[V]]] {}
      .where(new TypeParameter[K] {}, keyTypeDescriptorSupplier.get())
      .where(new TypeParameter[V] {}, valueTypeDescriptorSupplier.get())

}

final private class ScalaMapViewFn[K, V](
  keyTypeDescriptorSupplier: TypeDescriptorSupplier[K],
  valueTypeDescriptorSupplier: TypeDescriptorSupplier[V]
) extends ViewFn[MultimapView[Unit, (K, V)], Map[K, V]] {
  override def getMaterialization(): Materialization[MultimapView[Unit, (K, V)]] =
    Materializations.multimap()

  override def apply(primitiveViewT: MultimapView[Unit, (K, V)]): Map[K, V] =
    primitiveViewT.get(()).iterator().asScala.toMap

  override def getTypeDescriptor(): TypeDescriptor[Map[K, V]] =
    new TypeDescriptor[Map[K, V]] {}
      .where(new TypeParameter[K] {}, keyTypeDescriptorSupplier.get())
      .where(new TypeParameter[V] {}, valueTypeDescriptorSupplier.get())

}

final private class ScalaIterableViewFn[T](
  typeDescriptorSupplier: TypeDescriptorSupplier[T]
) extends ViewFn[MultimapView[Unit, T], Iterable[T]] {
  override def getMaterialization(): Materialization[MultimapView[Unit, T]] =
    Materializations.multimap()

  override def apply(primitiveViewT: MultimapView[Unit, T]): Iterable[T] =
    primitiveViewT.get(()).asScala

  override def getTypeDescriptor(): TypeDescriptor[Iterable[T]] =
    new TypeDescriptor[Iterable[T]] {}.where(new TypeParameter[T] {}, typeDescriptorSupplier.get())
}

final private class ScalaListViewFn[T](
  typeDescriptorSupplier: TypeDescriptorSupplier[T]
) extends ViewFn[MultimapView[Unit, T], List[T]] {
  override def getMaterialization(): Materialization[MultimapView[Unit, T]] =
    Materializations.multimap()

  override def apply(primitiveViewT: MultimapView[Unit, T]): List[T] =
    primitiveViewT.get(()).asScala.toList

  override def getTypeDescriptor(): TypeDescriptor[List[T]] =
    new TypeDescriptor[List[T]] {}.where(new TypeParameter[T] {}, typeDescriptorSupplier.get())
}

final private class AsScalaMultimap[K: Coder, V: Coder]
    extends AsScalaBase[(K, V), Map[K, Iterable[V]]] {
  override def view(
    coll: PCollection[KV[Unit, (K, V)]]
  ): PCollectionView[Map[K, Iterable[V]]] = {
    val pipeline = coll.getPipeline()
    val keyCoder =
      CoderMaterializer.beam(pipeline.getCoderRegistry, pipeline.getOptions(), Coder[K])
    val valueCoder =
      CoderMaterializer.beam(pipeline.getCoderRegistry, pipeline.getOptions(), Coder[V])
    ViewImpl(
      coll,
      new ScalaMultimapViewFn(
        () => keyCoder.getEncodedTypeDescriptor(),
        () => valueCoder.getEncodedTypeDescriptor()
      ),
      coll.getWindowingStrategy()
    )
  }
}

final private class AsScalaMap[K: Coder, V: Coder] extends AsScalaBase[(K, V), Map[K, V]] {
  override def view(
    coll: PCollection[KV[Unit, (K, V)]]
  ): PCollectionView[Map[K, V]] = {
    val pipeline = coll.getPipeline()
    val keyCoder =
      CoderMaterializer.beam(pipeline.getCoderRegistry, pipeline.getOptions(), Coder[K])
    val valueCoder =
      CoderMaterializer.beam(pipeline.getCoderRegistry, pipeline.getOptions(), Coder[V])
    ViewImpl(
      coll,
      new ScalaMapViewFn(
        () => keyCoder.getEncodedTypeDescriptor(),
        () => valueCoder.getEncodedTypeDescriptor()
      ),
      coll.getWindowingStrategy()
    )
  }
}

final private class AsScalaList[T: Coder] extends AsScalaBase[T, List[T]] {
  override def view(
    coll: PCollection[KV[Unit, T]]
  ): PCollectionView[List[T]] = {
    val coder = CoderMaterializer.beam(
      coll.getPipeline.getCoderRegistry,
      coll.getPipeline.getOptions(),
      Coder[T]
    )
    ViewImpl(
      coll,
      new ScalaListViewFn(() => coder.getEncodedTypeDescriptor()),
      coll.getWindowingStrategy()
    )
  }
}

final private class AsScalaIterable[T: Coder] extends AsScalaBase[T, Iterable[T]] {
  override def view(
    coll: PCollection[KV[Unit, T]]
  ): PCollectionView[Iterable[T]] = {
    val coder = CoderMaterializer.beam(
      coll.getPipeline.getCoderRegistry,
      coll.getPipeline.getOptions(),
      Coder[T]
    )
    ViewImpl(
      coll,
      new ScalaIterableViewFn(() => coder.getEncodedTypeDescriptor()),
      coll.getWindowingStrategy()
    )
  }
}

abstract private class AsScalaBase[T, U] extends PTransform[PCollection[T], PCollectionView[U]] {

  def view(coll: PCollection[KV[Unit, T]]): PCollectionView[U]

  override def expand(input: PCollection[T]): PCollectionView[U] =
    Try(GroupByKey.applicableTo(input)) match {
      case Success(_) =>
        val materializationInput = input.apply(new UnitKeyToMultimapMaterialization)
        val v = view(materializationInput)
        materializationInput.apply(CreatePCollectionView.of(v))
        v
      case Failure(e) =>
        throw new IllegalStateException("Unable to create a side-input view from input", e)
    }
}

final private class UnitKeyToMultimapMaterializationDoFn[T] extends DoFn[T, KV[Unit, T]] {
  @ProcessElement
  def processElement(c: DoFn[T, KV[Unit, T]]#ProcessContext): Unit = {
    c.output(KV.of((), c.element()))
    ()
  }
}

final private class UnitKeyToMultimapMaterialization[T]
    extends PTransform[PCollection[T], PCollection[KV[Unit, T]]] {

  private[this] val unitCoder =
    CoderMaterializer.beamWithDefault(com.spotify.scio.coders.Coder.unitCoder)

  override def expand(input: PCollection[T]): PCollection[KV[Unit, T]] = {
    val output = input.apply(ParDo.of(new UnitKeyToMultimapMaterializationDoFn[T]))
    output.setCoder(KvCoder.of(unitCoder, input.getCoder()))
    output
  }
}

object View {

  final def asScalaList[T: Coder]: PTransform[PCollection[T], PCollectionView[List[T]]] =
    new AsScalaList

  final def asScalaIterable[T: Coder]: PTransform[PCollection[T], PCollectionView[Iterable[T]]] =
    new AsScalaIterable

  final def asScalaMap[K: Coder, V: Coder]
    : PTransform[PCollection[(K, V)], PCollectionView[Map[K, V]]] =
    new AsScalaMap

  final def asScalaMultimap[K: Coder, V: Coder]
    : PTransform[PCollection[(K, V)], PCollectionView[Map[K, Iterable[V]]]] =
    new AsScalaMultimap
}
