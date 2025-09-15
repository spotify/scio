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

package com.spotify.scio.util

import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.values.SideInputContext
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import com.twitter.chill.ClosureCleaner

import scala.collection.compat._

private[scio] object FunctionsWithSideInput {
  trait SideInputDoFn[T, U] extends NamedDoFn[T, U] {
    def sideInputContext(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): SideInputContext[T] =
      // Workaround for type inference limit
      new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext], w)
  }

  trait SideInputDoFnWithResource[T, U, R] extends DoFnWithResource[T, U, R] with NamedFn {
    def sideInputContext(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): SideInputContext[T] =
      // Workaround for type inference limit
      new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext], w)
  }

  def filterFn[T](f: (T, SideInputContext[T]) => Boolean): DoFn[T, T] =
    new SideInputDoFn[T, T] {
      val g = ClosureCleaner.clean(f) // defeat closure

      /*
       * ProcessContext is required as an argument because it is passed to SideInputContext
       * */
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, T]#ProcessContext, w: BoundedWindow): Unit =
        if (g(c.element(), sideInputContext(c, w))) {
          c.output(c.element())
        }
    }

  def flatMapFn[T, U](f: (T, SideInputContext[T]) => TraversableOnce[U]): DoFn[T, U] =
    new SideInputDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure

      /*
       * ProcessContext is required as an argument because it is passed to SideInputContext
       * */
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): Unit = {
        val i = g(c.element(), sideInputContext(c, w)).iterator
        while (i.hasNext) c.output(i.next())
      }
    }

  def mapFn[T, U](f: (T, SideInputContext[T]) => U): DoFn[T, U] =
    new SideInputDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure

      /*
       * ProcessContext is required as an argument because it is passed to SideInputContext
       * */
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): Unit =
        c.output(g(c.element(), sideInputContext(c, w)))
    }

  def partialFn[T, U](f: PartialFunction[(T, SideInputContext[T]), U]): DoFn[T, U] =
    new SideInputDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure

      /*
       * ProcessContext is required as an argument because it is passed to SideInputContext
       * */
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): Unit = {
        if (g.isDefinedAt(c.element(), sideInputContext(c, w))) {
          c.output(g(c.element(), sideInputContext(c, w)))
        }
      }
    }

  class CollectFnWithResource[T, U, R](
    resource: => R,
    resourceType: ResourceType,
    pfn: PartialFunction[(R, T, SideInputContext[T]), U]
  ) extends SideInputDoFnWithResource[T, U, R] {
    override def getResourceType: ResourceType = resourceType
    override def createResource: R = resource
    val isDefined: ((R, T, SideInputContext[T])) => Boolean =
      ClosureCleaner.clean(pfn.isDefinedAt) // defeat closure
    val g: PartialFunction[(R, T, SideInputContext[T]), U] = ClosureCleaner.clean(pfn)

    /*
     * ProcessContext is required as an argument because it is passed to SideInputContext
     * */
    @ProcessElement
    def processElement(
      c: DoFn[T, U]#ProcessContext,
      w: BoundedWindow,
      @Element element: T,
      out: OutputReceiver[U]
    ): Unit =
      if (isDefined((getResource, element, sideInputContext(c, w)))) {
        out.output(g((getResource, element, sideInputContext(c, w))))
      }
  }

  class MapFnWithWithResource[T, U, R](
    resource: => R,
    resourceType: ResourceType,
    f: (R, T, SideInputContext[T]) => U
  ) extends SideInputDoFnWithResource[T, U, R] {
    override def getResourceType: ResourceType = resourceType
    override def createResource: R = resource
    val g = ClosureCleaner.clean(f)

    /*
     * ProcessContext is required as an argument because it is passed to SideInputContext
     * */
    @ProcessElement
    def processElement(
      c: DoFn[T, U]#ProcessContext,
      w: BoundedWindow,
      @Element element: T,
      out: OutputReceiver[U]
    ): Unit =
      out.output(g(getResource, element, sideInputContext(c, w)))
  }

  class FlatMapFnWithResource[T, U, R](
    resource: => R,
    resourceType: ResourceType,
    f: (R, T, SideInputContext[T]) => TraversableOnce[U]
  ) extends SideInputDoFnWithResource[T, U, R] {
    override def getResourceType: ResourceType = resourceType
    override def createResource: R = resource
    val g = ClosureCleaner.clean(f)

    /*
     * ProcessContext is required as an argument because it is passed to SideInputContext
     * */
    @ProcessElement
    def processElement(
      c: DoFn[T, U]#ProcessContext,
      w: BoundedWindow,
      @Element element: T,
      out: OutputReceiver[U]
    ): Unit = {
      val i = g(getResource, element, sideInputContext(c, w)).iterator
      while (i.hasNext) out.output(i.next())
    }
  }

  class FilterFnWithResource[T, R](
    resource: => R,
    resourceType: ResourceType,
    f: (R, T, SideInputContext[T]) => Boolean
  ) extends SideInputDoFnWithResource[T, T, R] {
    override def getResourceType: ResourceType = resourceType
    override def createResource: R = resource
    val g = ClosureCleaner.clean(f)

    /*
     * ProcessContext is required as an argument because it is passed to SideInputContext
     * */
    @ProcessElement
    def processElement(
      c: DoFn[T, T]#ProcessContext,
      w: BoundedWindow,
      @Element element: T,
      out: OutputReceiver[T]
    ): Unit =
      if (g(getResource, element, sideInputContext(c, w))) {
        out.output(element)
      }
  }
}
