#!/usr/bin/env python
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
    return list(string.uppercase.replace('F', '')[:n])


def mkTypes(n):
    return ', '.join(mkVals(n))

def mkBounds(n):
    return ', '.join(x + ': Coder[' + x + ']' for x in mkVals(n))


# Functions

def tupleFns(out, n):
    types = mkTypes(n)
    print >> out, '  implicit def tuple%sCoder[%s](implicit %s): Coder[(%s)] = Coder.gen[(%s)]' % (n, types, mkBounds(n), types, types)


def main(out):
    print >> out, textwrap.dedent('''
        /*
         * Copyright 2016 Spotify AB.
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

        // scalastyle:off cyclomatic.complexity
        // scalastyle:off file.size.limit
        // scalastyle:off line.size.limit
        // scalastyle:off method.length
        // scalastyle:off number.of.methods
        // scalastyle:off parameter.number

        package com.spotify.scio.coders

        trait TupleCoders {
        ''').replace('  # NOQA', '').lstrip('\n')

    N = 22
    for i in xrange(2, N + 1):
        tupleFns(out, i)

    print >> out, '}'
    print >> out, textwrap.dedent('''
        // scalastyle:on cyclomatic.complexity
        // scalastyle:on file.size.limit
        // scalastyle:on line.size.limit
        // scalastyle:on method.length
        // scalastyle:on number.of.methods
        // scalastyle:on parameter.number''')

if __name__ == '__main__':
    main(sys.stdout)
