/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import org.apache.avro.Schema
import org.apache.avro.specific.{SpecificFixed, SpecificRecord}

import java.io.{ObjectInput, ObjectOutput}

class A extends SpecificRecord {
  override def put(i: Int, v: Any): Unit = ???
  override def get(i: Int): AnyRef = ???
  override def getSchema: Schema = ???
}

class B extends SpecificFixed {
  override def getSchema: Schema = ???
  override def writeExternal(out: ObjectOutput): Unit = ???
  override def readExternal(in: ObjectInput): Unit = ???
}
