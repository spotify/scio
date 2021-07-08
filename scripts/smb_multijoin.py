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
    arg_list = ['{}: Coder'.format(element) for element in mkVals(n)]
    return ', '.join(['KEY: Coder'] + arg_list)


def mkFnArgs(n):
    return ', '.join(['keyClass: Class[KEY]'] +
        [x.lower() + ': SortedBucketIO.Read[%s]' % x
        for x in mkVals(n)])


def mkFnRetVal(n, aWrapper=None, otherWrapper=None):
    def wrap(wrapper, x):
        return x if wrapper is None else wrapper + '[%s]' % x
    vals = (wrap(aWrapper if x == 'A' else otherWrapper, x) for x in mkVals(n))
    return 'SCollection[(KEY, (%s))]' % ', '.join(vals)

# Functions

def sortCoGroup(out, n):
    vals = mkVals(n)

    args = mkFnArgs(n)

    print('\tdef sortMergeCoGroup[%s](%s): %s = {' % (
        mkClassTags(n), args + ', ' + 'targetParallelism: TargetParallelism', mkFnRetVal(n, 'Iterable', 'Iterable')),
          file=out)

    print('\t\tval input = SortedBucketIO', file=out)
    print('\t\t.read(keyClass)', file=out)
    print('\t\t.of(%s)' %vals[0].lower(), file=out)
    for x in vals[1:]:
        print('\t\t.and(%s)' % x.lower(), file=out)
    print('\t\t.withTargetParallelism(targetParallelism)', file=out)

    print('\t\tval (%s) = (%s)' % (
        ', '.join('tupleTag' + x for x in vals),
        ', '.join('%s.getTupleTag' % x.lower() for x in vals)),
          file=out)
    print('''
    val tfName = self.tfName
        
    self
    .wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", input))
    .withName(tfName)
    .map { kv =>
    val cgbkResult = kv.getValue
    (
    kv.getKey,
    (
        ''', file=out)

    for x in vals:
        print('       cgbkResult.getAll(tupleTag%s).asScala,' % x, file=out)

    print('        )', file=out)
    print('      )', file=out)

    print('    }', file=out)
    print('  }', file=out)
    print(file=out)

    print('\tdef sortMergeCoGroup[%s](%s): %s = {' % (
        mkClassTags(n), args, mkFnRetVal(n, 'Iterable', 'Iterable')),
          file=out)
    print('\t\tsortMergeCoGroup(keyClass, %s, TargetParallelism.auto())' % mkArgs(n), file=out)
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

        // generated with smb-multijoin.py

        package com.spotify.scio.smb.util

        import com.spotify.scio.ScioContext
        import com.spotify.scio.coders.Coder
        import com.spotify.scio.values._
        import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, TargetParallelism}
        
        import scala.jdk.CollectionConverters._

        final class SMBMultiJoin(@transient private val self: ScioContext) extends Serializable {
        ''').replace('  # NOQA', '').lstrip('\n'), file=out)

    N = 5
    for i in range(5, N + 1):
        sortCoGroup(out, i)
    print('}', file=out)

    print(textwrap.dedent('''
            object SMBMultiJoin {
                def apply(sc: ScioContext): SMBMultiJoin = new SMBMultiJoin(sc)
            }
        ''').rstrip('\n'), file=out)


if __name__ == '__main__':
    main(sys.stdout)
