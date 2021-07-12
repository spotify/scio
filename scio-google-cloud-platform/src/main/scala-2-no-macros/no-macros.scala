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

// This file is just there to provide empty implementation of empty trait
// and make sure the code still compiles without macros.
// The scala-2 folder needs to be replaced by scala-2-no-macros, either:
//  by setting unmanagedSourceDirectories in sbt, or simply by renaming the files.

package com.spotify.scio.bigquery {
  trait MockTypedBigQuery {}
  trait MockTypedTable {}
}

package com.spotify.scio.bigquery.client {
  trait TypedBigQuery {}
}

package com.spotify.scio.bigquery.syntax {
  trait ScioContextTypedSyntax {}
  trait SCollectionTypedSyntax {}
  trait Aliases {}
}

package com.spotify.scio.bigquery.dynamic.syntax {
  trait SCollectionTypedSyntax {}
}
