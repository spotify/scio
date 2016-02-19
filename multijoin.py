/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#!/usr/bin/env python
import string
import sys
import textwrap


# Utilities

def mkVals(n):
    return list(string.uppercase[:n])


def mkArgs(n):
    return ', '.join(map(lambda x: x.lower(), mkVals(n)))


def mkClassTags(n):
    return ', '.join(x + ': ClassTag' for x in ['KEY'] + mkVals(n))


def mkFnArgs(n):
    return ', '.join(x.lower() + ': SCollection[(KEY, %s)]' % x for x in mkVals(n))  # NOQA


def mkFnRetVal(n, aWrapper=None, otherWrapper=None):
    def wrap(wrapper, x):
        return x if wrapper is None else wrapper + '[%s]' % x
    vals = (wrap(aWrapper if x == 'A' else otherWrapper, x) for x in mkVals(n))
    return 'SCollection[(KEY, (%s))]' % ', '.join(vals)


# Functions

def cogroup(out, n):
    vals = mkVals(n)

    print >> out, '  def coGroup[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n, 'Iterable', 'Iterable'))

    print >> out, '    val (%s) = (%s)' % (
        ', '.join('tag' + x for x in vals),
        ', '.join('new TupleTag[%s]()' % x for x in vals))

    print >> out, '    val keyed = KeyedPCollectionTuple'
    print >> out, '      .of(tagA, a.toKV.internal)'
    for x in vals[1:]:
        print >> out, '      .and(tag%s, %s.toKV.internal)' % (x, x.lower())
    print >> out, '      .apply(CallSites.getCurrent, CoGroupByKey.create())'

    print >> out, '    a.context.wrap(keyed).map { kv =>'
    print >> out, '      val (k, r) = (kv.getKey, kv.getValue)'
    print >> out, '      (k, (%s))' % ', '.join('r.getAll(tag%s).asScala' % x for x in vals)  # NOQA
    print >> out, '    }'
    print >> out, '  }'
    print >> out


def join(out, n):
    print >> out, '  def apply[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n))
    print >> out, '    coGroup(%s)' % mkArgs(n)
    print >> out, '      .flatMap { kv =>'
    print >> out, '        for {'
    for (i, x) in enumerate(mkVals(n)):
        print >> out, '          %s <- kv._2._%d' % (x.lower(), i + 1)
    print >> out, '        } yield (kv._1, (%s))' % mkArgs(n)
    print >> out, '      }'
    print >> out, '  }'
    print >> out


def left(out, n):
    print >> out, '  def left[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n, None, 'Option'))
    print >> out, '    coGroup(%s)' % mkArgs(n)
    print >> out, '      .flatMap { kv =>'
    print >> out, '        for {'
    for (i, x) in enumerate(mkVals(n)):
        y = x.lower()
        j = i + 1
        if (i == 0):
            print >> out, '          %s <- kv._2._%d' % (y, j)
        else:
            print >> out, '          %s <- if (kv._2._%d.isEmpty) Iterable(None) else kv._2._%d.map(Option(_))' % (  # NOQA
                y, j, j)
    print >> out, '        } yield (kv._1, (%s))' % mkArgs(n)
    print >> out, '      }'
    print >> out, '  }'
    print >> out


def outer(out, n):
    print >> out, '  def outer[%s](%s): %s = {' % (
        mkClassTags(n), mkFnArgs(n), mkFnRetVal(n, 'Option', 'Option'))
    print >> out, '    coGroup(%s)' % mkArgs(n)
    print >> out, '      .flatMap { kv =>'
    print >> out, '        for {'
    for (i, x) in enumerate(mkVals(n)):
        y = x.lower()
        j = i + 1
        print >> out, '          %s <- if (kv._2._%d.isEmpty) Iterable(None) else kv._2._%d.map(Option(_))' % (  # NOQA
            y, j, j)
    print >> out, '        } yield (kv._1, (%s))' % mkArgs(n)
    print >> out, '      }'
    print >> out, '  }'
    print >> out


def main(out):
    print >> out, textwrap.dedent('''
        // generated with multijoin.py

        // scalastyle:off cyclomatic.complexity
        // scalastyle:off file.size.limit
        // scalastyle:off line.size.limit
        // scalastyle:off number.of.methods
        // scalastyle:off parameter.number

        package com.spotify.scio.util

        import com.google.cloud.dataflow.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}  # NOQA
        import com.google.cloud.dataflow.sdk.values.TupleTag
        import com.spotify.scio.values.SCollection

        import scala.collection.JavaConverters._
        import scala.reflect.ClassTag

        object MultiJoin {
        ''').replace('  # NOQA', '')

    N = 22
    for i in xrange(2, N + 1):
        cogroup(out, i)
    for i in xrange(2, N + 1):
        join(out, i)
    for i in xrange(2, N + 1):
        left(out, i)
    for i in xrange(2, N + 1):
        outer(out, i)
    print >> out, '}'
    print >> out, textwrap.dedent('''
        // scalastyle:on cyclomatic.complexity
        // scalastyle:on file.size.limit
        // scalastyle:on line.size.limit
        // scalastyle:on number.of.methods
        // scalastyle:on parameter.number''')

if __name__ == '__main__':
    main(sys.stdout)
