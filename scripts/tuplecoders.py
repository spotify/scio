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
    return list(string.ascii_uppercase.replace("F", "")[:n])


def mkBounds(n):
    return ", ".join(x + ": Coder" for x in mkVals(n))


# Functions


def coder_class(out, n, scala_version):
    t_type = f"T{string.ascii_uppercase[n-1]}"
    types = mkVals(n)
    bounds = mkBounds(n)
    print(
        f"""
final private[coders] class Tuple{n}Coder[{', '.join(types)}]({', '.join(f'val {t.lower()}c: BCoder[{t}]' for t in types)}) extends StructuredCoder[({', '.join(types)})] {{

  override def getCoderArguments: JList[_ <: BCoder[_]] = List(ac, bc).asJava

  @inline def onErrorMsg[{t_type}](msg: => (String, String))(f: => {t_type}): {t_type} =
    try {{
      f
    }} catch {{
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          s"Exception while trying to `${{msg._1}}` an instance" +
            s" of Tuple{n}: Can't decode field ${{msg._2}}"
        )
    }}

  override def encode(value: ({', '.join(types)}), os: OutputStream): Unit = {{
    {(os.linesep + '    ').join(f'onErrorMsg("encode" -> "_{idx}")({t.lower()}c.encode(value._{idx}, os))' for idx, t in enumerate(types, 1))}
  }}
  override def decode(is: InputStream): ({', '.join(types)}) = {{
    (
      {(',' + os.linesep + '      ').join(f'onErrorMsg("decode" -> "_{idx}")({t.lower()}c.decode(is))' for idx, t in enumerate(types, 1))}
    )
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
    {(' &&' + os.linesep + '      ').join(f'{t.lower()}c.consistentWithEquals()' for t in types)}

  override def structuralValue(value: ({', '.join(types)})): AnyRef =
    if (consistentWithEquals()) {{
      value.asInstanceOf[AnyRef]
    }} else {{
        (
          {(',' + os.linesep + '          ').join(f'{t.lower()}c.structuralValue(value._{idx})' for idx, t in enumerate(types, 1))}
        )
    }}

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: ({', '.join(types)})): Boolean =
    {(' &&'  + os.linesep + '      ').join(f'{t.lower()}c.isRegisterByteSizeObserverCheap(value._{idx})' for idx, t in enumerate(types, 1))}

  override def registerByteSizeObserver(value: ({', '.join(types)}), observer: ElementByteSizeObserver): Unit = {{
    {(os.linesep + '    ').join(f'{t.lower()}c.registerByteSizeObserver(value._{idx}, observer)' for idx, t in enumerate(types, 1))}
  }}
}}
    """,
        file=out,
    )


def implicits(out, n, scala_version):
    types = mkVals(n)

    def coder_transform(a):
        if len(a) == 1:
            if scala_version == "2.12":
                return f"""Coder.transform(C{a[0]}.value)({a[0].lower()}c => Coder.beam(new Tuple{n}Coder[{', '.join(types)}]({', '.join(t.lower() + 'c' for t in types)})))"""
            else:
                return f"""Coder.transform(C{a[0]})({a[0].lower()}c => Coder.beam(new Tuple{n}Coder[{', '.join(types)}]({', '.join(t.lower() + 'c' for t in types)})))"""
        else:
            if scala_version == "2.12":
                return (
                    f"""Coder.transform(C{a[0]}.value) {{ {a[0].lower()}c => """
                    + coder_transform(a[1:])
                    + "}"
                )
            else:
                return (
                    f"""Coder.transform(C{a[0]}) {{ {a[0].lower()}c => """
                    + coder_transform(a[1:])
                    + "}"
                )

    print(
        f"""
    implicit def tuple{n}Coder[{', '.join(types)}](implicit {', '.join(f'C{t}: Strict[Coder[{t}]]' if scala_version == '2.12' else f'C{t}: Coder[{t}]' for t in types)}): Coder[({', '.join(types)})] = {{
    {coder_transform(types)}
    }}""",
        file=out,
    )


def main(out, scala_version):
    print(
        textwrap.dedent(
            f"""
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

        import java.io.{{InputStream, OutputStream}}

        {'import shapeless.Strict' if scala_version == '2.12' else ''}
        import com.spotify.scio.coders.{{Coder, CoderStackTrace}}
        import org.apache.beam.sdk.coders.Coder.NonDeterministicException
        import org.apache.beam.sdk.coders.{{Coder => BCoder, _}}
        import org.apache.beam.sdk.util.common.ElementByteSizeObserver

        import scala.jdk.CollectionConverters._
        import java.util.{{List => JList}}
        """
        )
        .replace("  # NOQA", "")
        .lstrip("\n"),
        file=out,
    )

    N = 22

    for i in range(2, N + 1):
        coder_class(out, i, scala_version)

    print("trait TupleCoders {", file=out)
    for i in range(2, N + 1):
        implicits(out, i, scala_version)

    print("}", file=out)


if __name__ == "__main__":
    main(sys.stdout, sys.argv[1])
