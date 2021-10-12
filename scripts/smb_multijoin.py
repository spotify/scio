#!/usr/bin/env python3
#
#  Copyright 2021 Spotify AB.
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
    arg_list = ['{}: Coder'.format(element) for element in mkVals(n)]
    return ', '.join(['KEY: Coder'] + arg_list)

def mkTransformClassTags(n):
    return ', '.join(['KEY'] + mkVals(n))


def mkFnArgs(n):
    return ', '.join(['keyClass: Class[KEY]'] +
                     [x.lower() + ': SortedBucketIO.Read[%s]' % x
                      for x in mkVals(n)])


def fnRetValHelper(n, aWrapper=None, otherWrapper=None):
    def wrap(wrapper, x):
        return x if wrapper is None else wrapper + '[%s]' % x
    vals = (wrap(aWrapper if x == 'A' else otherWrapper, x) for x in mkVals(n))
    return vals


def mkCogroupFnRetVal(n, aWrapper=None, otherWrapper=None):
    vals = fnRetValHelper(n, aWrapper, otherWrapper)
    return 'SCollection[(KEY, (%s))]' % ', '.join(vals)

def mkTransformFnRetVal(n, aWrapper=None, otherWrapper=None):
    vals = fnRetValHelper(n, aWrapper, otherWrapper)
    return 'sortedBucketScioContext.SortMergeTransformReadBuilder[KEY, (%s)]' % ', '.join(vals)

def mkTupleTag(vals):
    return 'val (%s) = (%s)' % (
        ', '.join('tupleTag' + x for x in vals),
        ', '.join('%s.getTupleTag' % x.lower() for x in vals))
# Functions

def sortMergeCoGroup(out, n):
    vals = mkVals(n)
    args = mkFnArgs(n)

    print('\tdef sortMergeCoGroup[%s](%s): %s = {' % (
        mkClassTags(n), args + ', ' + 'targetParallelism: TargetParallelism', mkCogroupFnRetVal(n, 'Iterable', 'Iterable')),
          file=out)

    print('\t\tval input = SortedBucketIO', file=out)
    print('\t\t.read(keyClass)', file=out)
    print('\t\t.of(%s)' %vals[0].lower(), file=out)
    for x in vals[1:]:
        print('\t\t.and(%s)' % x.lower(), file=out)
    print('\t\t.withTargetParallelism(targetParallelism)', file=out)

    print('\t\t' + mkTupleTag(vals), file=out)
    print('''
    val tfName = self.tfName
        
    self
    .applyTransform(s"SMB CoGroupByKey@$tfName", input)
    .withName(tfName)
    .map { kv =>
      val cgbkResult = kv.getValue
      (
      kv.getKey,
        (''', file=out)

    print(',\n'.join(
        '\t\t\t\t\tcgbkResult.getAll(tupleTag%s).asScala' % x for x in vals), file=out)

    print('        )', file=out)
    print('      )', file=out)

    print('    }', file=out)
    print('  }', file=out)
    print(file=out)

    print('\tdef sortMergeCoGroup[%s](%s): %s = {' % (
        mkClassTags(n), args, mkCogroupFnRetVal(n, 'Iterable', 'Iterable')),
          file=out)
    print('\t\tsortMergeCoGroup(keyClass, %s, TargetParallelism.auto())' % mkArgs(n), file=out)
    print('  }', file=out)
    print(file=out)

def sortMergeTransform(out, n):
    vals = mkVals(n)
    args = mkFnArgs(n)

    print('\tdef sortMergeTransform[%s](%s): %s = {' % (
        mkTransformClassTags(n), args + ', ' + 'targetParallelism: TargetParallelism',  mkTransformFnRetVal(n, 'Iterable', 'Iterable')),
          file=out)

    print('\t\t' + mkTupleTag(vals),
          file=out)

    print('\t\tnew sortedBucketScioContext.SortMergeTransformReadBuilder(', file=out)
    print('\t\t\tSortedBucketIO', file=out)
    print('\t\t\t.read(keyClass)', file=out)
    print('\t\t\t.of(%s)' %vals[0].lower(), file=out)
    for x in vals[1:]:
        print('\t\t\t.and(%s)' % x.lower(), file=out)
    print('\t\t\t.withTargetParallelism(targetParallelism),', file=out)
    print('\t\t\tcgbkResult =>', file=out)
    print('\t\t\t(', file=out)
    print(',\n'.join(
        '\t\t\t\tcgbkResult.getAll(tupleTag%s).asScala' % x for x in vals), file=out)
    print('\t\t\t)', file=out)
    print('\t\t)', file=out)
    print('  }', file=out)
    print(file=out)

    print('\tdef sortMergeTransform[%s](%s): %s = {' % (
        mkTransformClassTags(n), args,  mkTransformFnRetVal(n, 'Iterable', 'Iterable')),
          file=out)
    print('\t\tsortMergeTransform(keyClass, %s, TargetParallelism.auto())' % mkArgs(n), file=out)
    print('  }', file=out)
    print(file=out)




def main(out):
    print(textwrap.dedent('''
        /*
         * Copyright 2021 Spotify AB.
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

        // generated with smb-multijoin.py

        package com.spotify.scio.smb.util

        import com.spotify.scio.ScioContext
        import com.spotify.scio.coders.Coder
        import com.spotify.scio.values._
        import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, TargetParallelism}
        import com.spotify.scio.smb.syntax.SortedBucketScioContext
        
        import scala.jdk.CollectionConverters._

        final class SMBMultiJoin(private val self: ScioContext) {
        
        val sortedBucketScioContext = new SortedBucketScioContext(self)
        ''').replace('  # NOQA', '').lstrip('\n'), file=out)

    N = 22
    for i in range(5, N + 1):
        sortMergeCoGroup(out, i)

    N = 22
    for i in range(4, N + 1):
        sortMergeTransform(out, i)

    print('}', file=out)

    print(textwrap.dedent('''
            object SMBMultiJoin {
                def apply(sc: ScioContext): SMBMultiJoin = new SMBMultiJoin(sc)
            }
        ''').rstrip('\n'), file=out)


if __name__ == '__main__':
    main(sys.stdout)
