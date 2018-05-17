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

package com.spotify.scio.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.spanner.{Mutation, Struct}
import com.google.common.collect.Iterators
import com.twitter.chill.KSerializer
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup

import scala.collection.JavaConverters._

/*
 * Serializer for Struct class.
 *
 * DO NOT USE: This should only be used for Spanner tests. The Struct
 * class contains an ImmutableList member, which is not serializable
 * by Kryo's CollectionSerializer.
 */
private class SpannerTestStructSerializer extends KSerializer[Struct] {

  def write(kryo: Kryo, output: Output, struct: Struct): Unit = {

    output.writeString(struct.getString("id"))
    output.writeString(struct.getString("firstName"))
    output.writeString(struct.getString("lastName"))
    output.writeString(struct.getString("status"))
  }

  def read(kryo: Kryo, input: Input, tpe: Class[Struct]): Struct = {

    Struct.newBuilder
      .set("id").to(input.readString)
      .set("firstName").to(input.readString)
      .set("lastName").to(input.readString)
      .set("status").to(input.readString)
      .build
  }
}

/*
 * Serializer for Mutation class.
 *
 * DO NOT USE: This should only be used for Spanner tests. The Mutation
 * class contains an ImmutableList member, which is not serializable
 * by Kryo's CollectionSerializer.
 */
private class SpannerTestMutationSerializer extends KSerializer[Mutation] {

  def write(kryo: Kryo, output: Output, mutation: Mutation): Unit = {

    output.writeString(mutation.getTable)
    mutation.getValues.forEach(value => output.writeString(value.toString))
  }

  def read(kryo: Kryo, input: Input, tpe: Class[Mutation]): Mutation = {

    val table = input.readString
    var mutationBuilder = Mutation.newInsertBuilder(table)

    if (table == "usersBackup") {
      mutationBuilder
        .set("id").to(input.readString)
        .set("firstName").to(input.readString)
        .set("lastName").to(input.readString)

    } else if (table == "usersActive") {
      mutationBuilder.set("id").to(input.readString)
    }

    mutationBuilder.build
  }
}

/*
 * Serializer for MutationGroup class.
 *
 * DO NOT USE: This should only be used for Spanner tests. The
 * MutationGroup class contains an ImmutableList member, which is
 * not serializable by Kryo's CollectionSerializer.
 */
private class SpannerTestMutationGroupSerializer extends KSerializer[MutationGroup] {

  def write(kryo: Kryo, output: Output, mutationGroup: MutationGroup): Unit = {

    output.writeInt(Iterators.size(mutationGroup.iterator))

    mutationGroup.iterator.forEachRemaining{mutation =>
      output.writeString(mutation.getTable)
      mutation.getValues.forEach(value => output.writeString(value.toString))
    }
  }

  def read(kryo: Kryo, input: Input, tpe: Class[MutationGroup]): MutationGroup = {

    val mutationCount = input.readInt
    var mutations: List[Mutation] = Nil

    for (i <- 1 to mutationCount) {
      val table = input.readString
      var mutationBuilder = Mutation.newInsertBuilder(table)

      if (table == "usersBackup") {
        mutationBuilder
          .set("id").to(input.readString)
          .set("firstName").to(input.readString)
          .set("lastName").to(input.readString)

      } else if (table == "usersActive") {
        mutationBuilder.set("id").to(input.readString)
      }

      mutations = mutations :+ mutationBuilder.build
    }

    MutationGroup.create(mutations.head, mutations.tail.asJava)
  }
}
