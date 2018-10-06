package com.spotify.scio.coders.instances

import com.google.protobuf.{ByteString, Message}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.extensions.protobuf.{ByteStringCoder, ProtoCoder}

import scala.reflect.{classTag, ClassTag}

//
// Protobuf Coders
//
trait ProtobufCoders {

  implicit def bytestringCoder: Coder[ByteString] =
    Coder.beam(ByteStringCoder.of())

  implicit def protoMessageCoder[T <: Message: ClassTag]: Coder[T] =
    Coder.beam(ProtoCoder.of(classTag[T].runtimeClass.asInstanceOf[Class[T]]))
}
