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

  private def resolveBeamDependencies(deps: Seq[(String, String)]): Map[String, Set[String]] =
    deps
      .filter(d => d._1.startsWith("org.apache.beam"))
      .map {
        case (orgName, rev) =>
          beamDeps.computeIfAbsent(
            orgName,
            new JFunction[String, Map[String, Set[String]]] {
              override def apply(key: String): Map[String, Set[String]] = {
                val output = s"coursier resolve $key:$rev" lineStream_!

                output
                  .flatMap { dep =>
                    dep.split(":").toList match {
                      case org :: name :: rev :: _ => Some((s"$org:$name", rev))
                      case _                       => None
                    }
                  }
                  .toList
                  .groupBy(_._1)
                  .mapValues(_.map(_._2).toSet)
              }
            }
          )
      }
      .foldLeft(Map.empty[String, Set[String]])(_ ++ _)

  override lazy val projectSettings = Seq(
    checkBeamDependencies := {
      val deps = libraryDependencies.value.map(m => (s"${m.organization}:${m.name}", m.revision))
      val beamDependencies = resolveBeamDependencies(deps)
      val projectBeamDeps = deps
        .map(dep => (dep, beamDependencies.getOrElse(dep._1, Nil)))
        .collect {
          case ((dep, version), beamVersions) => beamVersions.map(v => (dep, (version, v)))
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
