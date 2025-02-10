/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill

class LeftSerializer[A, B] extends KSerializer[Left[A, B]] {
  def write(kser: Kryo, out: Output, left: Left[A, B]): Unit =
    kser.writeClassAndObject(out, left.left.get)

  def read(kser: Kryo, in: Input, cls: Class[Left[A, B]]): Left[A, B] =
    Left(kser.readClassAndObject(in).asInstanceOf[A])
}

class RightSerializer[A, B] extends KSerializer[Right[A, B]] {
  def write(kser: Kryo, out: Output, right: Right[A, B]): Unit =
    kser.writeClassAndObject(out, right.right.get)

  def read(kser: Kryo, in: Input, cls: Class[Right[A, B]]): Right[A, B] =
    Right(kser.readClassAndObject(in).asInstanceOf[B])
}
