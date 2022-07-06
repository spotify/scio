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

package com.spotify.scio.testing

import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import scala.jdk.CollectionConverters._
import com.spotify.scio.{registerSysProps, SysProp}
import scala.util.Try
import pprint.PPrinter
import com.google.api.client.json.GenericJson
import org.apache.beam.sdk.extensions.gcp.util.Transport

import scala.collection.compat._ // scalafix:ok

@registerSysProps
object PrettySysProps {
  val PrettyPrint: SysProp =
    SysProp("tests.prettyprint.colors", "Should pretty printed values be rendered with colors")
}

object Pretty {
  import pprint.Tree
  import fansi.{Color, Str}

  private def renderFieldName(n: String) =
    Tree.Lazy(_ => List(Color.LightBlue(n).toString).iterator)

  private def renderGenericRecord: PartialFunction[GenericRecord, Tree] = { case g =>
    val renderer =
      new pprint.Renderer(
        printer.defaultWidth,
        printer.colorApplyPrefix,
        printer.colorLiteral,
        printer.defaultIndent
      )
    def render(tree: Tree): Str =
      Str.join(renderer.rec(tree, 0, 0).iter.iterator.to(Iterable))
    Tree.Lazy { _ =>
      val fields =
        for {
          f <- g.getSchema().getFields().asScala
        } yield Str(
          render(renderFieldName(f.name)),
          ": ",
          render(treeifyAvro(g.get(f.name())))
        )
      List(
        Color.LightGray("{ ").toString +
          fields.reduce((a, b) => Str(a, ", ", b)) +
          Color.LightGray(" }")
      ).iterator
    }
  }

  private def renderSpecificRecord: PartialFunction[SpecificRecordBase, Tree] = { case x =>
    val fs =
      for {
        f <- x.getSchema().getFields().asScala
      } yield Tree.Infix(renderFieldName(f.name), "=", treeifyAvro(x.get(f.name())))
    Tree.Apply(x.getClass().getSimpleName(), fs.iterator)
  }

  private def treeifyAvro: PartialFunction[Any, Tree] = {
    case x: SpecificRecordBase =>
      renderSpecificRecord(x)
    case g: GenericRecord =>
      renderGenericRecord(g)
    case x =>
      printer.treeify(x, true, true)
  }

  private[this] val jsonFactory = Transport.getJsonFactory

  private def treeifyGenericJson: PartialFunction[GenericJson, Tree] = { case x =>
    pprint.Tree.Literal(jsonFactory.toString(x))
  }

  private val handlers: PartialFunction[Any, Tree] = {
    case x: GenericRecord => treeifyAvro(x)
    case x: GenericJson   => treeifyGenericJson(x)
  }

  private val useColors =
    PrettySysProps.PrettyPrint.valueOption
      .flatMap(x => Try(x.toBoolean).toOption)
      .getOrElse {
        // Crude test to check if the terminal seems to support colors
        (System.console() != null) && (System.getenv().get("TERM") != null)
      }

  val printer: PPrinter =
    if (useColors) {
      pprint.PPrinter(
        additionalHandlers = handlers
      )
    } else {
      pprint.PPrinter(
        additionalHandlers = handlers,
        colorLiteral = fansi.Attrs.Empty,
        colorApplyPrefix = fansi.Attrs.Empty
      )
    }
}
