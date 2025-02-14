/*
Copyright 2019 Twitter, Inc.

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

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.collection.immutable.{
  ArraySeq,
  HashMap,
  HashSet,
  ListMap,
  ListSet,
  NumericRange,
  Queue
}
import scala.runtime.VolatileByteRef

object SerializedExamplesData {
  val Examples = Seq(
    0 -> ("AgI=" -> Int.box(1)),
    1 -> ("AwFhYuM=" -> "abc"),
    2 -> ("BD+AAAA=" -> Float.box(1)),
    3 -> ("BQE=" -> Boolean.box(true)),
    4 -> ("BgE=" -> Byte.box(1)),
    5 -> ("BwBh" -> Char.box('a')),
    6 -> ("CAAB" -> Short.box(1)),
    7 -> ("CQI=" -> Long.box(1)),
    8 -> ("Cj/wAAAAAAAA" -> Double.box(1)),
    // 9 -> void is a special case
    // Note: Instead of JavaConverters.***Converter(***).as***, in Scala 2.12
    // methods JavaConverters.*** can be used directly. For backwards compatibility,
    // the legacy methods to convert are used here.
    10 -> ("DAF4AQECBA==" ->
      JavaConverters.seqAsJavaListConverter(Seq(2)).asJava), // Wrappers$SeqWrapper
    11 -> ("DQEBAHNjYWxhLmNvbGxlY3Rpb24uQXJyYXlPcHMkQXJyYXlJdGVyYXRvciRtY0kkc/ABAgBiAQIEBA==" ->
      JavaConverters.asJavaIteratorConverter(Iterator(2)).asJava), // Wrappers$IteratorWrapper
    12 -> ("DgEaAQEnAQIEAgQ=" ->
      JavaConverters.mapAsJavaMapConverter(Map(2 -> 2)).asJava), // Wrappers$MapWrapper
    13 -> ("DwEBAGphdmEudXRpbC5Db2xsZWN0aW9ucyRTaW5nbGV0b25MaXP0AQIE" ->
      JavaConverters
        .asScalaBufferConverter(_root_.java.util.Collections.singletonList(2))
        .asScala), // Wrappers$JListWrapper
    14 -> ("EAEBAGphdmEudXRpbC5Db2xsZWN0aW9ucyRTaW5nbGV0b25NYfABAgQCBA==" ->
      JavaConverters
        .mapAsScalaMapConverter(_root_.java.util.Collections.singletonMap(2, 2))
        .asScala), // Wrappers$JMapWrapper
    15 -> ("EQECBA==" -> Some(2)),
    16 -> ("EgECBA==" -> Left(2)),
    17 -> ("EwECBA==" -> Right(2)),
    // 18 -> ("FAEBAgQ=" -> Vector(2)),
    // new vector classes in 2.13 see 144, 145
    19 -> ("FQEBAgQ=" -> Set(2)),
    20 -> ("FgECAgQCBg==" -> Set(2, 3)),
    21 -> ("FwEDAgQCBgII" -> Set(2, 3, 4)),
    22 -> ("GAEEAgQCBgIIAgo=" -> Set(2, 3, 4, 5)),
    23 -> ("GQEA" -> HashSet.empty[Any]),
    24 -> ("GgEBJwECBAIG" -> Map(2 -> 3)),
    25 -> ("GwECJwECBAIGJwECCAIK" -> Map(2 -> 3, 4 -> 5)),
    26 -> ("HAEDJwECBAIGJwECCAIKJwECDAIO" -> Map(2 -> 3, 4 -> 5, 6 -> 7)),
    27 -> ("HQEEJwECBAIGJwECCAIKJwECDAIOJwECEAIS" -> Map(2 -> 3, 4 -> 5, 6 -> 7, 8 -> 9)),
    28 -> ("HgEA" -> HashMap.empty[Any, Any]),
    29 -> ("HwEMAAwIBgI=" -> new Range.Inclusive(3, 6, 1)),
    30 -> ("IAECAgoAAAEAAQBzY2FsYS5tYXRoLk51bWVyaWMkSW50SXNJbnRlZ3JhbKQBAQADAgQCAg==" ->
      new NumericRange.Inclusive[Int](2, 5, 1)),
    31 -> ("IQECAgoAAAAAAQBzY2FsYS5tYXRoLk51bWVyaWMkSW50SXNJbnRlZ3JhbKQBAQADAgQCAg==" ->
      new NumericRange.Exclusive[Int](2, 5, 1)),
    32 -> ("IgECAgYCCg==" -> scala.collection.mutable.BitSet(3, 5)),
    33 -> ("IwEBJwECBgIK" -> scala.collection.mutable.HashMap(3 -> 5)),
    34 -> ("JAEBAgY=" -> scala.collection.mutable.HashSet(3)),
    35 -> ("JQF4AQECBA==" -> Seq(2).asJavaCollection), // Wrappers$IterableWrapper
    36 -> ("JgEDAYJh" -> Tuple1("a")),
    37 -> ("JwEDAYJhAwGCYg==" -> ("a", "b")),
    38 -> ("KAECAgIEAgY=" -> (1, 2, 3)),
    39 -> ("KQECAgIEAgYCCA==" -> (1, 2, 3, 4)),
    40 -> ("KgECAgIEAgYCCAIK" -> (1, 2, 3, 4, 5)),
    41 -> ("KwECAgIEAgYCCAIKAgw=" -> (1, 2, 3, 4, 5, 6)),
    42 -> ("LAECAgIEAgYCCAIKAgwCDg==" -> (1, 2, 3, 4, 5, 6, 7)),
    43 -> ("LQECAgIEAgYCCAIKAgwCDgIQ" -> (1, 2, 3, 4, 5, 6, 7, 8)),
    44 -> ("LgECAgIEAgYCCAIKAgwCDgIQAhI=" -> (1, 2, 3, 4, 5, 6, 7, 8, 9)),
    45 -> ("LwECAgIEAgYCCAIKAgwCDgIQAhICAA==" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0)),
    46 -> ("MAECAgIEAgYCCAIKAgwCDgIQAhICAAIC" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1)),
    47 -> ("MQECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQ=" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2)),
    48 -> ("MgECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBg==" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3)),
    49 -> ("MwECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgII" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3,
      4)),
    50 -> ("NAECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgo=" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3,
      4, 5)),
    51 -> ("NQECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDA==" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
      2, 3, 4, 5, 6)),
    52 -> ("NgECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDAIO" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
      2, 3, 4, 5, 6, 7)),
    53 -> ("NwECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDAIOAhA=" -> (1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
      1, 2, 3, 4, 5, 6, 7, 8)),
    54 -> ("OAECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDAIOAhACEg==" -> (1, 2, 3, 4, 5, 6, 7, 8, 9,
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
    55 -> ("OQECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDAIOAhACEgIA" -> (1, 2, 3, 4, 5, 6, 7, 8, 9,
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0)),
    56 -> ("OgECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDAIOAhACEgIAAgI=" -> (1, 2, 3, 4, 5, 6, 7,
      8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1)),
    57 -> ("OwECAgIEAgYCCAIKAgwCDgIQAhICAAICAgQCBgIIAgoCDAIOAhACEgIAAgICBA==" -> (1, 2, 3, 4, 5, 6,
      7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2)),
    58 -> ("PAEAAAAAAAAAAQ==" -> Tuple1(1L)),
    59 -> ("PQEAAAAB" -> Tuple1(1)),
    60 -> ("PgE/8AAAAAAAAA==" -> Tuple1(1.0)),
    61 -> ("PwEAAAAAAAAAAQAAAAAAAAAC" -> (1L, 2L)),
    62 -> ("QAEAAAAAAAAAAQAAAAI=" -> (1L, 2)),
    63 -> ("QQEAAAAAAAAAAUAAAAAAAAAA" -> (1L, 2.0)),
    64 -> ("QgEAAAABAAAAAAAAAAI=" -> (1, 2L)),
    65 -> ("QwEAAAABAAAAAg==" -> (1, 2)),
    66 -> ("RAEAAAABQAAAAAAAAAA=" -> (1, 2.0)),
    67 -> ("RQE/8AAAAAAAAAAAAAAAAAAC" -> (1.0, 2L)),
    68 -> ("RgE/8AAAAAAAAAAAAAI=" -> (1.0, 2)),
    69 -> ("RwE/8AAAAAAAAEAAAAAAAAAA" -> (1.0, 2.0)),
    70 -> ("SAGCYQ==" -> Symbol("a")),
    // 71 -> interface scala.reflect.ClassTag
    72 -> ("SgE=" -> runtime.BoxedUnit.UNIT),
    73 -> ("SwEDawICAgQCBg==" -> _root_.java.util.Arrays.asList(1, 2, 3)),
    74 -> ("TAECAAAAAAAAAAAAAAAAAAAAAA==" -> new _root_.java.util.BitSet(65)),
    75 -> ("TQEAAA==" -> new _root_.java.util.PriorityQueue[Int](7)),
    76 -> ("TgFhKuI=" -> _root_.java.util.regex.Pattern.compile("a*b")),
    77 -> ("TwEA" -> new _root_.java.sql.Date(0)),
    78 -> ("UAEH" -> new _root_.java.sql.Time(7)),
    79 -> ("UQEDwI23AQ==" -> new _root_.java.sql.Timestamp(3)),
    80 -> ("UgGB" -> new _root_.java.net.URI("")),
    81 -> ("UwEwLjAuMC6wAg==" -> new _root_.java.net.InetSocketAddress(2)),
    82 -> ("VAECBA==" -> new _root_.java.util.UUID(1, 2)),
    83 -> ("VQGs7QAFc3IAEGphdmEudXRpbC5Mb2NhbGV++BFgnDD57AMABkkACGhhc2hjb2RlTAAHY291bnRyeXQAEkxqYXZhL2xhbmcvU3RyaW5nO0wACmV4dGVuc2lvbnNxAH4AAUwACGxhbmd1YWdlcQB+AAFMAAZzY3JpcHRxAH4AAUwAB3ZhcmlhbnRxAH4AAXhw/////3QAAHEAfgADdAACZW5xAH4AA3EAfgADeA==" ->
      _root_.java.util.Locale.ENGLISH),
    // 84 -> class java.text.SimpleDateFormat - this case has two very special aspects:
    // a) SimpleDateFormat("") serializes to about 40.000 bytes
    // b) each time you serialize SimpleDateFormat(""), you get a slightly different binary representation.
    // Probably, one should write a custom serializer for this class...
    85 -> ("VwEBAGphdmEudXRpbC5Db2xsZWN0aW9ucyRFbXB0eUxpc/QB" ->
      _root_.java.util.Collections.unmodifiableCollection(_root_.java.util.Collections.EMPTY_LIST)),
    86 -> ("WAEBAGphdmEudXRpbC5Db2xsZWN0aW9ucyRFbXB0eUxpc/QB" ->
      _root_.java.util.Collections.unmodifiableList(_root_.java.util.Collections.EMPTY_LIST)),
    87 -> ("WQEBAGphdmEudXRpbC5MaW5rZWRMaXP0AQA=" ->
      _root_.java.util.Collections.unmodifiableList(new _root_.java.util.LinkedList[Int]())),
    88 -> ("WgEBAGphdmEudXRpbC5Db2xsZWN0aW9ucyRTaW5nbGV0b25NYfABAgICBA==" ->
      _root_.java.util.Collections
        .unmodifiableMap[Int, Int](_root_.java.util.Collections.singletonMap(1, 2))),
    89 -> ("WwEBAGphdmEudXRpbC5Db2xsZWN0aW9ucyRFbXB0eVNl9AE=" ->
      _root_.java.util.Collections.unmodifiableSet(_root_.java.util.Collections.EMPTY_SET)),
    90 -> ("XAEBAMEBamF2YS51dGlsLkNvbGxlY3Rpb25zJFVubW9kaWZpYWJsZU5hdmlnYWJsZU1hcCRFbXB0eU5hdmlnYWJsZU1hcAEA" ->
      _root_.java.util.Collections
        .unmodifiableSortedMap[Int, Int](_root_.java.util.Collections.emptySortedMap())),
    // 91 -> class java.util.Collections$UnmodifiableSortedSet
    // With the following implementation, we have a problem
    // 91 -> ("XQEBAMEBamF2YS51dGlsLkNvbGxlY3Rpb25zJFVubW9kaWZpYWJsZU5hdmlnYWJsZVNldCRFbXB0eU5hdmlnYWJsZVNldAEA" ->
    //   _root_.java.util.Collections.unmodifiableSortedSet[Int](_root_.java.util.Collections.emptySortedSet())),
    // because we get an exception in the test with the root cause:
    // com.spotify.scio.vendor.chill.Instantiators$ can not access a member of class java.util.Collections$UnmodifiableNavigableSet$EmptyNavigableSet with modifiers "public"
    // 92 -> class com.esotericsoftware.kryo.serializers.ClosureSerializer$Closure"""
    93 -> ("XwEGAAQEAgI=" -> collection.immutable.Range(1, 3)),
    94 -> ("YAECgA==" -> Array(Byte.MinValue)),
    95 -> ("YQECf/8=" -> Array(Short.MaxValue)),
    96 -> ("YgEC/////w8=" -> Array(Int.MinValue)),
    97 -> ("YwEC/v//////////" -> Array(Long.MaxValue)),
    98 -> ("ZAECAAAAAQ==" -> Array(Float.MinPositiveValue)),
    99 -> ("ZQEC/+////////8=" -> Array(Double.MinValue)),
    100 -> ("ZgECAQ==" -> Array(true)),
    101 -> ("ZwECAHg=" -> Array('x')),
    102 -> ("aAECAWNh9A==" -> Array("cat")),
    103 -> ("aQEDAgQDAW1vdXPl" -> Array(2, "mouse")),
    104 -> ("agECAQ==" -> classOf[Int]),
    105 -> ("awE=" -> new Object()),
    106 -> ("bAEBBgFgAQKA" -> wrapByteArray(Array(Byte.MinValue))),
    107 -> ("bQEBCAFhAQJ//w==" -> wrapShortArray(Array(Short.MaxValue))),
    108 -> ("bgEBAgFiAQL/////Dw==" -> wrapIntArray(Array(Int.MinValue))),
    109 -> ("bwEBCQFjAQL+//////////8=" -> wrapLongArray(Array(Long.MaxValue))),
    110 -> ("cAEBBAFkAQIAAAAB" -> wrapFloatArray(Array(Float.MinPositiveValue))),
    111 -> ("cQEBCgFlAQL/7////////w==" -> wrapDoubleArray(Array(Double.MinValue))),
    112 -> ("cgEBBQFmAQIB" -> wrapBooleanArray(Array(true))),
    113 -> ("cwEBBwFnAQIAeA==" -> wrapCharArray(Array('x'))),
    114 -> ("dAEBAwBoAQIBY2H0" -> wrapRefArray(Array("cat"))),
    115 -> ("dQE=" -> None),
    116 -> ("dgEA" -> collection.immutable.Queue()),
    117 -> ("dwEA" -> Nil),
    118 -> ("eAEBAgQ=" -> (2 :: Nil)),
    // 119 -> ("eAEGAAQEAgI=" -> collection.immutable.Range(1, 3)),
    120 -> ("egEBdGHj" -> wrapString("tac")),
    121 -> ("ewECfwECBAIG" -> collection.immutable.TreeSet(3, 2)),
    122 -> ("fAEBfwEnAQIGAgQ=" -> collection.immutable.TreeMap(3 -> 2)),
    123 -> ("fQE=" -> math.Ordering.Byte),
    124 -> ("fgE=" -> math.Ordering.Short),
    125 -> ("fwE=" -> math.Ordering.Int),
    126 -> ("gAEB" -> math.Ordering.Long),
    127 -> ("gQEB" -> math.Ordering.Float),
    128 -> ("ggEB" -> math.Ordering.Double),
    129 -> ("gwEB" -> math.Ordering.Boolean),
    130 -> ("hAEB" -> math.Ordering.Char),
    131 -> ("hQEB" -> math.Ordering.String),
    132 -> ("hgEBAA==" -> Set[Any]()),
    133 -> ("hwEBAA==" -> ListSet[Any]()),
    134 -> ("iAEBAUgBgmE=" -> ListSet[Any]('a)),
    135 -> ("iQEBAA==" -> Map[Any, Any]()),
    136 -> ("igEBAA==" -> ListMap[Any, Any]()),
    137 -> ("iwEBAScBSAGCYUgE" -> ListMap('a -> 'a)),
    138 -> ("jAEBeAEBAgI=" -> Stream(1)),
    139 -> ("jQEB" -> Stream()),
    140 -> ("jgEBCg==" -> new VolatileByteRef(10)),
    141 -> ("jwEBAQBqYXZhLm1hdGguQmlnRGVjaW1h7AECAgA=" -> math.BigDecimal(2)),
    142 -> ("kAEBAA==" -> (Queue.empty[Any], true)),
    143 -> ("kQEBAQIC" -> (Map(1 -> 2).keySet, true)),
    144 -> ("kgEBAA==" -> Vector.empty[Int]),
    145 -> ("kwEBAQIC" -> Vector(1)),
    146 -> ("lAEBQAIC" + "AgIC" * 42 -> Vector.fill(1 << 5 + 1)(1)),
    // Skip BigVectors. too slow
    // 147 -> ("" -> Vector.fill(1 << 10 + 1)(1)),
    // 148 -> ("" -> Vector.fill(1 << 15 + 1)(1)),
    // 149 -> ("" -> Vector.fill(1 << 20 + 1)(1)),
    // 150 -> ("" -> Vector.fill(1 << 25 + 1)(1)),
    151 -> ("mQEBAQKA" -> ArraySeq(Byte.MinValue)),
    152 -> ("mgEBAQJ//w==" -> ArraySeq(Short.MaxValue)),
    153 -> ("mwEBAQL/////Dw==" -> ArraySeq(Int.MinValue)),
    154 -> ("nAEBAQL+//////////8=" -> ArraySeq(Long.MaxValue)),
    155 -> ("nQEBAQIAAAAB" -> ArraySeq(Float.MinPositiveValue)),
    156 -> ("ngEBAQL/7////////w==" -> ArraySeq(Double.MinValue)),
    157 -> ("nwEBAQIB" -> ArraySeq(true)),
    158 -> ("oAEBAQIAeA==" -> ArraySeq('x')),
    159 -> ("oQEBaAECAWNh9A==" -> ArraySeq("cat"))
  )

  val SpecialCasesNotInExamplesMap: Seq[Int] = Seq(9, 18, 71, 84, 91, 92, 119, 147, 148, 149, 150)

  val OmitExamplesInScalaVersion: Map[String, Seq[Int]] = Map.empty
}
