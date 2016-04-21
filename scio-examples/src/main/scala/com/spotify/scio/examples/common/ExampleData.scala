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

package com.spotify.scio.examples.common

object ExampleData {

  val SHAKESPEARE_PATH = "gs://dataflow-samples/shakespeare/"
  val SHAKESPEARE_ALL = "gs://dataflow-samples/shakespeare/*"
  val KING_LEAR = "gs://dataflow-samples/shakespeare/kinglear.txt"
  val OTHELLO = "gs://dataflow-samples/shakespeare/othello.txt"

  val EXPORTED_WIKI_TABLE = "gs://dataflow-samples/wikipedia_edits/*.json"
  val MONTHS = "gs://dataflow-samples/samples/misc/months.txt"
  val TRAFFIC =
    "gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv"
  val GAMING = "gs://dataflow-samples/game/gaming_data*.csv"

  val WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations"
  val SHAKESPEARE_TABLE = "publicdata:samples.shakespeare"
  val EVENT_TABLE = "clouddataflow-readonly:samples.gdelt_sample"
  val COUNTRY_TABLE = "gdelt-bq:full.crosswalk_geocountrycodetohuman"

}
