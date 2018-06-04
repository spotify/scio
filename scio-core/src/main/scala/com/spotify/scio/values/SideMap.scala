package com.spotify.scio.values

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class SideMap[K, V](side: SideInput[mutable.Map[K, ArrayBuffer[V]]]) {
  def toSideKeySet: SideSet[K] = SideSet[K](side.asInstanceOf[SideInput[mutable.Map[K, Any]]])
}

case class SideSet[K](side: SideInput[mutable.Map[K, Any]]) {
  def toSideMap: SideMap[K, Any] =
    SideMap[K, Any](side.asInstanceOf[SideInput[mutable.Map[K, ArrayBuffer[Any]]]])
}