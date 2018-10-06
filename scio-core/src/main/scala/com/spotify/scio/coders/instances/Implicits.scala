package com.spotify.scio.coders.instances

trait Implicits
    extends TupleCoders
    with ScalaCoders
    with AvroCoders
    with ProtobufCoders
    with JavaCoders
    with AlgebirdCoders
    with JodaCoders
    with Serializable
    with LowPriorityFallbackCoder
