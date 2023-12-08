/*
 * Copyright 2020 Spotify AB
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

package com.spotify.scio.transforms

import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}

import scala.collection.compat._

class CollectFnWithResource[T, U, R] private[transforms] (
  resource: => R,
  resourceType: ResourceType,
  pfn: PartialFunction[(R, T), U]
) extends DoFnWithResource[T, U, R] {
  override def getResourceType: ResourceType = resourceType

  override def createResource: R = resource

  val isDefined: ((R, T)) => Boolean = ClosureCleaner.clean(pfn.isDefinedAt) // defeat closure
  val g: PartialFunction[(R, T), U] = ClosureCleaner.clean(pfn)
  @ProcessElement
  def processElement(@Element element: T, out: OutputReceiver[U]): Unit =
    if (isDefined((getResource, element))) {
      out.output(g((getResource, element)))
    }
}

class MapFnWithResource[T, U, R] private[transforms] (
  resource: => R,
  resourceType: ResourceType,
  f: (R, T) => U
) extends DoFnWithResource[T, U, R] {
  override def getResourceType: ResourceType = resourceType

  override def createResource: R = resource

  val g: (R, T) => U = ClosureCleaner.clean(f)

  @ProcessElement
  def processElement(
    @Element element: T,
    out: OutputReceiver[U]
  ): Unit =
    out.output(g(getResource, element))
}

class FlatMapFnWithResource[T, U, R] private[transforms] (
  resource: => R,
  resourceType: ResourceType,
  f: (R, T) => TraversableOnce[U]
) extends DoFnWithResource[T, U, R] {
  override def getResourceType: ResourceType = resourceType

  override def createResource: R = resource

  val g: (R, T) => TraversableOnce[U] = ClosureCleaner.clean(f)
  @ProcessElement
  def processElement(
    @Element element: T,
    out: OutputReceiver[U]
  ): Unit = {
    val i = g(getResource, element).iterator
    while (i.hasNext) out.output(i.next())
  }
}

class FilterFnWithResource[T, R] private[transforms] (
  resource: => R,
  resourceType: ResourceType,
  f: (R, T) => Boolean
) extends DoFnWithResource[T, T, R] {
  override def getResourceType: ResourceType = resourceType

  override def createResource: R = resource

  val g: (R, T) => Boolean = ClosureCleaner.clean(f)
  @ProcessElement
  def processElement(@Element element: T, out: OutputReceiver[T]): Unit =
    if (g(getResource, element)) {
      out.output(element)
    }
}
