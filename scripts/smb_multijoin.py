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

def mkTypes(n):
    return list(string.ascii_uppercase[:n])

def mkVals(n):
    return [x.lower() for x in mkTypes(n)]

def mkRawClassTags(n):
    return ', '.join(['KEY'] + mkTypes(n))

def mkClassTags(n):
    arg_list = ['{}: Coder'.format(element) for element in mkTypes(n)]
    return ', '.join(['KEY: Coder'] + arg_list)

def mkReadArgs(n):
    return ', '.join('%s: SortedBucketIO.Read[%s]' % (x.lower(), x) for x in mkTypes(n))

def mkFnArgs(n):
    return 'keyClass: Class[KEY], ' + mkReadArgs(n)

def fnRetValHelper(n, aWrapper=None, otherWrapper=None):
    def wrap(wrapper, x):
        return x if wrapper is None else wrapper + '[%s]' % x
    vals = (wrap(aWrapper if x == 'A' else otherWrapper, x) for x in mkTypes(n))
    return vals


def mkCogroupFnRetVal(n, aWrapper=None, otherWrapper=None):
    vals = fnRetValHelper(n, aWrapper, otherWrapper)
    return 'SCollection[(KEY, (%s))]' % ', '.join(vals)

def mkTransformFnRetVal(n, aWrapper=None, otherWrapper=None):
    vals = fnRetValHelper(n, aWrapper, otherWrapper)
    return 'SortMergeTransform.ReadBuilder[KEY, KEY, Void, (%s)]' % ', '.join(vals)

def mkTupleTag(n):
    return ['val tupleTag%s = %s.getTupleTag' % (x, x.lower()) for x in mkTypes(n)]
# Functions

def sortMergeCoGroup(out, n):
    types = mkTypes(n)
    vals = mkVals(n)
    args = ', '.join(vals)
    fnArgs = mkFnArgs(n)

    print('\tdef sortMergeCoGroup[%s](%s): %s = self.requireNotClosed {' % (
        mkClassTags(n),
        fnArgs + ', ' + 'targetParallelism: TargetParallelism',
        mkCogroupFnRetVal(n, 'Iterable', 'Iterable')),
      file=out)

    print('\t\tval tfName = self.tfName''', file=out)
    print('\t\tval keyed = if (self.isTest) {', file=out)
    print('\t\t\ttestCoGroup[KEY](%s)' % args, file=out)
    print('\t\t} else {', file=out)
    print('\t\t\tval transform = SortedBucketIO', file=out)
    print('\t\t\t\t.read(keyClass)', file=out)
    print('\t\t\t\t.of(%s)' % args, file=out)
    print('\t\t\t\t.withTargetParallelism(targetParallelism)', file=out)
    print('\t\t\tself.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))', file=out)
    print('\t\t}')
    print('\t\t' + '\n\t\t'.join(mkTupleTag(n)), file=out)
    print('\t\tkeyed', file=out)
    print('\t\t\t.withName(tfName)', file=out)
    print('\t\t\t.map { kv =>', file=out)
    print('\t\t\t\tval result = kv.getValue', file=out)
    print('\t\t\t\t(', file=out)
    print('\t\t\t\t\tkv.getKey(),', file=out)
    print('\t\t\t\t\t(', file=out)
    print('\t\t\t\t\t\t' + ',\n\t\t\t\t\t\t'.join('result.getAll(tupleTag%s).asScala' % x for x in types), file=out)
    print('\t\t\t\t\t)', file=out)
    print('\t\t\t\t)', file=out)
    print('\t\t\t}', file=out)
    print('\t}', file=out)
    print(file=out)

    print('\tdef sortMergeCoGroup[%s](%s): %s = {' % (
        mkClassTags(n),
        fnArgs,
        mkCogroupFnRetVal(n, 'Iterable', 'Iterable')),
      file=out)
    print('\t\tsortMergeCoGroup(keyClass, %s, TargetParallelism.auto())' % args, file=out)
    print('\t}', file=out)
    print(file=out)

def sortMergeTransform(out, n):
    types = mkTypes(n)
    vals = mkVals(n)
    args = ', '.join(vals)
    fnArgs = mkFnArgs(n)

    print('\tdef sortMergeTransform[%s](%s): %s = self.requireNotClosed {' % (
        mkClassTags(n),
        fnArgs + ', ' + 'targetParallelism: TargetParallelism',
        mkTransformFnRetVal(n, 'Iterable', 'Iterable')),
      file=out)



    print('\t\t' + '\n\t\t'.join(mkTupleTag(n)), file=out)
    print('\t\tval fromResult = { (result: CoGbkResult) =>', file=out)
    print('\t\t\t(', file=out)
    print('\t\t\t\t' + ',\n\t\t\t\t'.join('result.getAll(tupleTag%s).asScala' % x for x in types), file=out)
    print('\t\t\t)', file=out)
    print('\t\t}', file=out)
    print('\t\tif (self.isTest) {', file=out)
    print('\t\t\tval result = testCoGroup[KEY](%s)' % args, file=out)
    print('\t\t\tval keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))', file=out)
    print('\t\t\tnew SortMergeTransform.ReadBuilderTest(self, keyed)', file=out)
    print('\t\t} else {', file=out)
    print('\t\t\tval transform = SortedBucketIO', file=out)
    print('\t\t\t\t.read(keyClass)', file=out)
    print('\t\t\t\t.of(%s)' % args, file=out)
    print('\t\t\t\t.withTargetParallelism(targetParallelism)', file=out)
    print('\t\t\tnew SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)', file=out)
    print('\t\t}', file=out)
    print('\t}', file=out)
    print(file=out)

    print('\tdef sortMergeTransform[%s](%s): %s = {' % (
        mkClassTags(n),
        fnArgs,
        mkTransformFnRetVal(n, 'Iterable', 'Iterable')),
      file=out)
    print('\t\tsortMergeTransform(keyClass, %s, TargetParallelism.auto())' % args, file=out)
    print('\t}', file=out)
    print(file=out)

def testCoGroup(out):
    print('''
\tprivate[smb] def testCoGroup[K](
\t\treads: SortedBucketIO.Read[_]*
\t): SCollection[KV[K, CoGbkResult]] = {
\t\tval testInput = TestDataManager.getInput(self.testId.get)
\t\tval read :: rs = reads.asInstanceOf[Seq[SortedBucketIO.Read[Any]]].toList: @nowarn
\t\tval test = testInput[(K, Any)](SortedBucketIOUtil.testId(read)).toSCollection(self)
\t\tval keyed = rs
\t\t\t.foldLeft(KeyedPCollectionTuple.of(read.getTupleTag, test.toKV.internal)) { (kpt, r) =>
\t\t\t\tval c = testInput[(K, Any)](SortedBucketIOUtil.testId(r)).toSCollection(self)
\t\t\t\tkpt.and(r.getTupleTag, c.toKV.internal)
\t\t\t}
\t\t\t.apply(CoGroupByKey.create())
\t\tself.wrap(keyed)
\t}''', file=out)



def main(out):
    print('''
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
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketIOUtil, TargetParallelism}
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.KV
import com.spotify.scio.smb.SortMergeTransform
import org.typelevel.scalaccompat.annotation.nowarn

import scala.jdk.CollectionConverters._
final class SMBMultiJoin(private val self: ScioContext) {'''.lstrip('\n'), file=out)

    N = 22
    for i in range(2, N + 1):
        sortMergeCoGroup(out, i)

    for i in range(2, N + 1):
        sortMergeTransform(out, i)

    testCoGroup(out)

    print('}', file=out)

    print('''
object SMBMultiJoin {
\tfinal def apply(sc: ScioContext): SMBMultiJoin = new SMBMultiJoin(sc)
}''', file=out)


if __name__ == '__main__':
    main(sys.stdout)
