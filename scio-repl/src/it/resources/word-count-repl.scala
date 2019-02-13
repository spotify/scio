/*
 * Copyright 2016 Spotify AB.
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

val readme = sc.textFile("README.md")
val wc = readme.flatMap(_.split("[^a-zA-Z]+")).filter(_.nonEmpty).map(_.toLowerCase).countByValue.materialize
val scioResult = sc.close().waitUntilDone()
val w = scioResult.tap(wc).value.maxBy(_._2)._1
println(s"SUCCESS: [$w]")
