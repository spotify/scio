package com.spotify.scio

import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

package object named {
  implicit class ApplyName[T: ClassTag](val self: SCollection[T]) {
    def |(name: String): SCollection[T] =
      self.asInstanceOf[LazySCollection[T]].withName(name)
  }
}
