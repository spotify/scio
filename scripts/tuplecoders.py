#!/usr/bin/env python3
#
#  Copyright 2016 Spotify AB.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import string
import sys
import textwrap
import os


# Utilities

def mkVals(n):
    return list(string.ascii_uppercase.replace('F', '')[:n])

def mkBounds(n):
    return ', '.join(x + ': Coder' for x in mkVals(n))

# Functions

def tupleFns(out, n):
    t_type = f'T{string.ascii_uppercase[n-1]}'
    types = mkVals(n)
    bounds = mkBounds(n)

    def coder_transform(a):
        if len(a) == 1:
            return f'''Coder.transform(C{a[0]})({a[0].lower()}c => Coder.beam(new Tuple{n}Coder[{','.join(types)}]({','.join(t.lower() + 'c' for t in types)})))'''
        else:
            return f'''Coder.transform(C{a[0]}) {{ {a[0].lower()}c =>''' + coder_transform(a[1:]) + "}"
        
    print(f'''    
final private class Tuple{n}Coder[{','.join(types)}]({','.join(f'{t.lower()}c: BCoder[{t}]' for t in types)}) extends AtomicCoder[({','.join(types)})] {{
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[{t_type}](msg: => (String, String))(f: => {t_type}): {t_type} =
    try {{
      f
    }} catch {{
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${{msg._1}}` an instance" +
              s" of Tuple{n}: Can't decode field ${{msg._2}}"
          ),
          materializationStackTrace
        )
    }}

  override def encode(value: ({','.join(types)}), os: OutputStream): Unit = {{
    {os.linesep.join(f'onErrorMsg("encode" -> "_{idx}")({t.lower()}c.encode(value._{idx}, os))' for idx, t in enumerate(types, 1))}
  }}
  override def decode(is: InputStream): ({','.join(types)}) = {{
    ({','.join(f'onErrorMsg("decode" -> "_{idx}")({t.lower()}c.decode(is))' for idx, t in enumerate(types, 1))})
  }}

  override def toString: String =
    s"Tuple{n}Coder({', '.join(f'_{idx} -> ${t.lower()}c' for idx, t in enumerate(types, 1))})"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {{
    val cs = List({', '.join(f'"_{idx}" -> {t.lower()}c' for idx, t in enumerate(types, 1))})
    val problems = cs.flatMap {{ case (label, c) =>
      try {{
        c.verifyDeterministic()
        Nil
      }} catch {{
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }}
    }}

    problems match {{
      case (_, e) :: _ =>
        val reasons = problems.map {{ case (reason, _) => reason }}
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }}
  }}

  override def consistentWithEquals(): Boolean =
    {' && '.join(f'{t.lower()}c.consistentWithEquals()' for t in types)}

  override def structuralValue(value: ({','.join(types)})): AnyRef =
    if (consistentWithEquals()) {{
      value.asInstanceOf[AnyRef]
    }} else {{
        ({','.join(f'{t.lower()}c.structuralValue(value._{idx})' for idx, t in enumerate(types, 1))})
    }}

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: ({','.join(types)})): Boolean =
    {' && '.join(f'{t.lower()}c.isRegisterByteSizeObserverCheap(value._{idx})' for idx, t in enumerate(types, 1))}

  override def registerByteSizeObserver(value: ({','.join(types)}), observer: ElementByteSizeObserver): Unit = {{
    {os.linesep.join(f'{t.lower()}c.registerByteSizeObserver(value._{idx}, observer)' for idx, t in enumerate(types, 1))}
  }}
}}
    
    implicit def tuple{n}Coder[{','.join(types)}](implicit {','.join(f'C{t}: Coder[{t}]' for t in types)}): Coder[({','.join(types)})] = {{
    {coder_transform(types)}
    }}''', file=out)


def main(out):
    print(textwrap.dedent('''
        /*
         * Copyright 2020 Spotify AB.
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

        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // !! generated with tuplecoders.py
        // !! DO NOT EDIT MANUALLY
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    
        package com.spotify.scio.coders.instances

        import java.io.{InputStream, OutputStream}

        import com.spotify.scio.coders.{Coder, CoderStackTrace}
        import org.apache.beam.sdk.coders.Coder.NonDeterministicException
        import org.apache.beam.sdk.coders.{Coder => BCoder, _}
        import org.apache.beam.sdk.util.common.ElementByteSizeObserver

        import scala.jdk.CollectionConverters._

        trait TupleCoders {
        ''').replace('  # NOQA', '').lstrip('\n'), file=out)

    N = 22
    for i in range(3, N + 1):
        tupleFns(out, i)

    print('}', file=out)

if __name__ == '__main__':
    main(sys.stdout)
