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

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}

import sbt._
import Keys._
import sbt.plugins.JvmPlugin

import scala.sys.process._

object CheckBeamDependencies extends AutoPlugin {
  override def requires: JvmPlugin.type = sbt.plugins.JvmPlugin
  override def trigger: PluginTrigger = allRequirements

  object autoImport {
    val checkBeamDependencies = taskKey[Unit]("check beam dependencies")
  }
  import autoImport._

  private[this] val beamDeps = new ConcurrentHashMap[String, Map[String, Set[String]]]()

  private val AnsiCode = s"\\x1b\\[[0-9;]*m".r

  private def coursierResolveTree(coordinate: String): List[String] =
    Seq("sh", "-c", s"""coursier resolve --tree "$coordinate" 2>/dev/null""").lineStream_!.toList

  // Parses direct (first-level) dependencies from beam module POMs via `coursier resolve --tree`.
  // Only direct deps are meaningful; transitive resolution picks up unrelated version conflicts.
  private def resolveBeamDependencies(deps: Seq[(String, String)]): Map[String, Set[String]] =
    deps
      .filter(_._1.startsWith("org.apache.beam"))
      .map { case (orgName, rev) =>
        beamDeps.computeIfAbsent(
          orgName,
          new JFunction[String, Map[String, Set[String]]] {
            override def apply(key: String): Map[String, Set[String]] = {
              coursierResolveTree(s"$key:$rev")
                .map(l => AnsiCode.replaceAllIn(l, ""))
                .filter(l =>
                  l.length > 3 &&
                    l.startsWith("   ") &&
                    (l.charAt(3) == '├' || l.charAt(3) == '└')
                )
                .flatMap { line =>
                  val coord = line.dropWhile(c => !c.isLetterOrDigit)
                  coord.split(":").toList match {
                    case org :: name :: rest :: _ if !org.startsWith("org.apache.beam") =>
                      Some((s"$org:$name", rest.split(" -> ").head.trim))
                    case _ => None
                  }
                }
                .groupBy(_._1)
                .mapValues(_.map(_._2).toSet)
                .toMap
            }
          }
        )
      }
      .foldLeft(Map.empty[String, Set[String]]) { case (acc, m) =>
        m.foldLeft(acc) { case (a, (k, v)) =>
          a + (k -> (a.getOrElse(k, Set.empty[String]) ++ v))
        }
      }

  override lazy val projectSettings = Seq(
    checkBeamDependencies := {
      val deps = libraryDependencies.value.map(m => (s"${m.organization}:${m.name}", m.revision))
      val beamDependencies = resolveBeamDependencies(deps)
      val projectBeamDeps = deps
        .map(dep => (dep, beamDependencies.getOrElse(dep._1, Nil)))
        .collect { case ((dep, version), beamVersions) =>
          beamVersions.map(v => (dep, (version, v)))
        }
        .flatten

      streams.value.log.warn {
        (thisProjectRef.value.project :: projectBeamDeps.collect {
          case (org, (v1, v2)) if v1 != v2 =>
            s"* $org:$v1 -> beam: $v2"
        }.toList).mkString("\n")
      }
    }
  )
}
