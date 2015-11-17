package com.spotify.scio.examples.common

object ExampleData {

  val SHAKESPEARE_PATH = "gs://dataflow-samples/shakespeare/"
  val SHAKESPEARE_ALL = "gs://dataflow-samples/shakespeare/*"
  val KING_LEAR = "gs://dataflow-samples/shakespeare/kinglear.txt"
  val OTHELLO = "gs://dataflow-samples/shakespeare/othello.txt"

  val EXPORTED_WIKI_TABLE = "gs://dataflow-samples/wikipedia_edits/*.json"
  val MONTHS = "gs://dataflow-samples/samples/misc/months.txt"
  val TRAFFIC = "gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv"

  val WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations"
  val SHAKESPEARE_TABLE = "publicdata:samples.shakespeare"
  val EVENT_TABLE = "clouddataflow-readonly:samples.gdelt_sample"
  val COUNTRY_TABLE = "gdelt-bq:full.crosswalk_geocountrycodetohuman"

}
