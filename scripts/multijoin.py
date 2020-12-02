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


# Utilities

def mkVals(n):
    return list(string.ascii_uppercase[:n])


def mkArgs(n):
    return ', '.join(map(lambda x: x.lower(), mkVals(n)))


def mkClassTags(n):
    return ', '.join(['KEY'] + mkVals(n))


def mkFnArgs(n):
    return ', '.join(
        x.lower() + ': SCollection[(KEY, %s)]' % x
        for x in mkVals(n))


def mkFnRetVal(n, aWrapper=None, otherWrapper=None):
    def wrap(wrapper, x):
        return x if wrapper is None else wrapper + '[%s]' % x
    vals = (wrap(aWrapper if x == 'A' else otherWrapper, x) for x in mkVals(n))
    return 'SCollection[(KEY, (%s))]' % ', '.join(vals)


def common(out, vals):
    print('    implicit val keyCoder: Coder[KEY] = a.keyCoder', file=out)
    print('    implicit val (%s) = (%s)' % (
        ', '.join('coder' + x + ': Coder[' + x + ']' for x in vals),
        ', '.join('%s.valueCoder' % x.lower() for x in vals)),
        file=out)

    print('    val (%s) = (%s)' % (
        ', '.join('tag' + x for x in vals),
        ', '.join('new TupleTag[%s]()' % x for x in vals)),
        file=out)

    print('    val keyed = KeyedPCollectionTuple', file=out)
    print('      .of(tagA, a.toKV.internal)', file=out)
    for x in vals[1:]:
        print('      .and(tag%s, %s.toKV.internal)' % (x, x.lower()), file=out)
    print(
        '      .apply(s"CoGroupByKey@$tfName", CoGroupByKey.create())',
        file=out)


# Functions

def cogroup(out, n):
    vals = mkVals(n)

    print('  def cogroup[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n, 'Iterable', 'Iterable')),
        file=out)

    common(out, vals)

    print('    a.context.wrap(keyed).withName(tfName).map { kv =>', file=out)
    print('      val (key, result) = (kv.getKey, kv.getValue)', file=out)
    print('      (key, (%s))' % ', '.join(
        'result.getAll(tag%s).asScala' % x for x in vals),
        file=out)  # NOQA
    print('    }', file=out)
    print('  }', file=out)
    print(file=out)


def join(out, n):
    vals = mkVals(n)

    print('  def apply[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n)), file=out)

    common(out, vals)

    print(
        '    a.context.wrap(keyed).withName(tfName).flatMap { kv =>',
        file=out)
    print('      val (key, result) = (kv.getKey, kv.getValue)', file=out)
    print('      for {', file=out)
    for x in reversed(vals):
        print('        %s <- result.getAll(tag%s).asScala.iterator' % (
            x.lower(), x),
            file=out)
    print('      } yield (key, (%s))' % mkArgs(n), file=out)
    print('    }', file=out)
    print('  }', file=out)
    print(file=out)


def left(out, n):
    vals = mkVals(n)
    print('  def left[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n, None, 'Option')),
        file=out)

    common(out, vals)

    print(
        '    a.context.wrap(keyed).withName(tfName).flatMap { kv =>',
        file=out)
    print('      val (key, result) = (kv.getKey, kv.getValue)', file=out)
    print('      for {', file=out)
    for (i, x) in enumerate(reversed(vals)):
        if (i == n - 1):
            print('        %s <- result.getAll(tag%s).asScala.iterator' % (
                x.lower(), x),
                file=out)
        else:
            print('        %s <- toOptions(result.getAll(tag%s).asScala.iterator)' % ( # NOQA
                x.lower(), x),
                file=out)
    print('      } yield (key, (%s))' % mkArgs(n), file=out)
    print('    }', file=out)
    print('  }', file=out)
    print(file=out)


def outer(out, n):
    vals = mkVals(n)
    print('  def outer[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n, 'Option', 'Option')),
        file=out)

    common(out, vals)

    print(
        '    a.context.wrap(keyed).withName(tfName).flatMap { kv =>',
        file=out)
    print('      val (key, result) = (kv.getKey, kv.getValue)', file=out)
    print('      for {', file=out)
    for (i, x) in enumerate(reversed(vals)):
        print(
            '        %s <- toOptions(result.getAll(tag%s).asScala.iterator)' % (x.lower(), x), # NOQA
            file=out)
    print('      } yield (key, (%s))' % mkArgs(n), file=out)
    print('    }', file=out)
    print('  }', file=out)
    print(file=out)


def main(out):
    print(textwrap.dedent('''
        /*
         * Copyright 2019 Spotify AB.
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

        // generated with multijoin.py

        package com.spotify.scio.util

        import com.spotify.scio.values.SCollection
        import com.spotify.scio.coders.Coder
        import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}  # NOQA
        import org.apache.beam.sdk.values.TupleTag

        import scala.jdk.CollectionConverters._
        import com.spotify.scio.values.SCollection.makePairSCollectionFunctions

        trait MultiJoin extends Serializable {

          protected def tfName: String = CallSites.getCurrent

          def toOptions[T](xs: Iterator[T]): Iterator[Option[T]] = if (xs.isEmpty) Iterator(None) else xs.map(Option(_))
        ''').replace('  # NOQA', '').lstrip('\n'), file=out)

    N = 22
    for i in range(2, N + 1):
        cogroup(out, i)
    for i in range(2, N + 1):
        join(out, i)
    for i in range(2, N + 1):
        left(out, i)
    for i in range(2, N + 1):
        outer(out, i)
    print('}', file=out)
    print(textwrap.dedent('''
        object MultiJoin extends MultiJoin {
          def withName(name: String): MultiJoin = new NamedMultiJoin(name)
        }

        private class NamedMultiJoin(val name: String) extends MultiJoin {
          override def tfName: String = name
        }
        ''').rstrip('\n'), file=out)


if __name__ == '__main__':
    main(sys.stdout)
