package com.spotify.scio.coders

import org.apache.avro.specific.SpecificRecordBase

import scala.reflect.macros.blackbox

private[coders] object AvroCoderMacros {

  /**
    * Generate a coder which does not serialize the schema and relies exclusively on types.
    */
  def staticInvokeCoder[T <: SpecificRecordBase: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    val companionSymbol = companioned.companion
    val companionType = companionSymbol.typeSignature

    q"""
    _root_.com.spotify.scio.coders.Coder.beam(
      _root_.org.apache.beam.sdk.coders.AvroCoder.of[$companioned](
        classOf[$companioned],
        ${companionType}.getClassSchema()
      )
    )
    """
  }

}

