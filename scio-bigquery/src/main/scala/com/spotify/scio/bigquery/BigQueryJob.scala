/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.bigquery

import com.google.api.services.bigquery.model._

/** A BigQueryJob */
sealed trait BigQueryJob {
  val jobReference: Option[JobReference]
  val jobType: String
  val table: TableReference
}

/** Extract Job Container */
private[scio] case class ExtractJob(destinationUris: List[String],
                                    jobReference: Option[JobReference],
                                    table: TableReference)
    extends BigQueryJob {

  val jobType = "Extract"
}

/** Load Job Container */
private[scio] case class LoadJob(sources: List[String],
                                 jobReference: Option[JobReference],
                                 table: TableReference)
    extends BigQueryJob {

  val jobType = "Load"
}

/** A query job that may delay execution. */
private[scio] case class QueryJob(query: String,
                                  jobReference: Option[JobReference],
                                  table: TableReference)
    extends BigQueryJob {
  val jobType = "Query"
}
