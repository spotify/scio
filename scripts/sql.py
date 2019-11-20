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
    return list(string.uppercase.replace("R", "").replace("Q", "")[:n])


def mkLowerVals(n):
    return ", ".join(x.lower() for x in mkVals(n))


def mkTypes(n):
    return ", ".join(mkVals(n))


def mkBounds(n, bound):
    return ", ".join(x + ": " + bound for x in mkVals(n))


def mkSetSchema(n):
    return "\n".join(
        [
            "val coll{x1} = Sql.setSchema({x2})".format(x1=x, x2=x.lower())
            for x in mkVals(n)
        ]
    )


def mkTupleTagArgs(n):
    return ", ".join(
        ["{x2}Tag: TupleTag[{x1}]".format(x1=x, x2=x.lower()) for x in mkVals(n)]
    )


def mkVarTag(n):
    return ", ".join(["{}Tag".format(x.lower()) for x in mkVals(n)])


def mkQVarTag(n):
    return ", ".join(["q.{}Tag".format(x.lower()) for x in mkVals(n)])


def mkSCollection(n):
    return ", ".join(
        ["{x1}: SCollection[{x2}]".format(x1=x.lower(), x2=x) for x in mkVals(n)]
    )


def mkPCollectionTuple(n):
    stuff = ".and".join(
        ["(q.{x1}Tag, coll{x2}.internal)".format(x1=x.lower(), x2=x) for x in mkVals(n)]
    )
    tf_name = " join ".join(["${{coll{}.tfName}}".format(x) for x in mkVals(n)])
    return """
            PCollectionTuple
            .of{stuff}
            .apply(s"{tf_name}", sqlTransform)
    """.format(
        stuff=stuff, tf_name=tf_name
    )


def mkValsFmt(n, fmt):
    return [
        fmt.format(upperIdx=x, lowerIdx=x.lower(), idx=idx + 1)
        for idx, x in enumerate(mkVals(n))
    ]


# Functions


def sqlCollectionFns(out, idx):
    print >> out, """
    import com.spotify.scio.schemas._
    import com.spotify.scio.values.SCollection
    import org.apache.beam.sdk.extensions.sql.SqlTransform
    import org.apache.beam.sdk.extensions.sql.impl.ParseException
    import org.apache.beam.sdk.values._

    import scala.language.experimental.macros

    final case class Query{n}[{types}, R](query: String, {tuple_tag_args}, udfs: List[Udf] = Nil)

    object Query{n} {{
        import scala.reflect.macros.blackbox
        import QueryMacros._

        {typecheck}

        {macro}
    }}

    final class SqlSCollection{n}[{bounds}]({scollections}) {{

    def query(q: String, {tuple_tag_args}, udfs: Udf*): SCollection[Row] =
        query(Query{n}(q, {var_tags}, udfs.toList))

    def query(q: Query{n}[{types}, Row]): SCollection[Row] = {{
        a.context.wrap {{
        {set_coll_schemas}
        val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

        {pcollection_tuple}
        }}
    }}

    def queryAs[R: Schema](q: String, {tuple_tag_args}, udfs: Udf*): SCollection[R] =
        queryAs(Query{n}(q, {var_tags}, udfs.toList))

    def queryAs[R: Schema](q: Query{n}[{types}, R]): SCollection[R] =
        try {{
        query(q.query, {q_var_tags}, q.udfs: _*).to(To.unchecked((_, i) => i))
        }} catch {{
        case e: ParseException =>
            Query{n}.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
        }}

    }}""".format(
        n=idx,
        types=mkTypes(idx),
        bounds=mkBounds(idx, "Schema"),
        set_coll_schemas=mkSetSchema(idx),
        tuple_tag_args=mkTupleTagArgs(idx),
        var_tags=mkVarTag(idx),
        q_var_tags=mkQVarTag(idx),
        scollections=mkSCollection(idx),
        pcollection_tuple=mkPCollectionTuple(idx),
        typecheck=mkTypecheck(idx),
        macro=mkMacro(idx),
    )


def mkFrom(idx):
    return """def from[{bounds}]({scollections}): SqlSCollection{n}[{types}] = new SqlSCollection{n}({vals})""".format(
        n=idx,
        types=mkTypes(idx),
        bounds=mkBounds(idx, "Schema"),
        scollections=mkSCollection(idx),
        vals=mkLowerVals(idx),
    )


def mkMaterializeSchema(n):
    return ", ".join(
        "(q.{x1}Tag.getId, SchemaMaterializer.beamSchema[{x2}])".format(
            x1=x.lower(), x2=x
        )
        for x in mkVals(n)
    )


def mkTypecheck(n):
    return """
      def typecheck[{bounds}, R: Schema](q: Query{n}[{types}, R]): Either[String, Query{n}[{types}, R]] =
        Queries.typecheck(
        q.query,
        List({schema_tuples}),
        SchemaMaterializer.beamSchema[R],
        q.udfs
        ).right.map(_ => q)

      def typed[{bounds}, R: Schema](query: String, {tuple_tag_args}): Query{n}[{types}, R] =
        macro typed{n}Impl[{types}, R]
    """.format(
        n=n,
        types=mkTypes(n),
        bounds=mkBounds(n, "Schema"),
        schema_tuples=mkMaterializeSchema(n),
        tuple_tag_args=mkTupleTagArgs(n),
    )


def mkMacro(n):
    return """
      def typed{n}Impl[{weak_bounds}, R: c.WeakTypeTag](c: blackbox.Context)(query: c.Expr[String], {expr_tuple_tag})({expr_schema}, rSchema: c.Expr[Schema[R]]): c.Expr[Query{n}[{types}, R]] = {{
    val h = new {{ val ctx: c.type = c }} with SchemaMacroHelpers
    import h._
    import c.universe._

    {assert_concrete}
    assertConcrete[R](c)

    val ({schema_tuple_vals}, schemas{n_p}) =
        FastEval(c)(
            c.Expr[({schemas}, Schema[R])](
                q"({infer_schemas}, ${{untyped(rSchema)}})"))

    val sq = Query{n}[{types}, R](cons(c)(query), {tuple_tags})
    typecheck(sq)({schema_tuple_vals}, schemas{n_p})
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ => c.Expr[Query{n}[{types}, R]](q"_root_.com.spotify.scio.sql.Query{n}($query, {tag_trees})")
      )
  }}""".format(
        n=n,
        n_p=n + 1,
        types=mkTypes(n),
        weak_bounds=mkBounds(n, "c.WeakTypeTag"),
        schemas=", ".join(mkValsFmt(n, "Schema[{upperIdx}]")),
        schema_tuple_vals=", ".join(mkValsFmt(n, "schemas{idx}")),
        infer_schemas=", ".join(mkValsFmt(n, "${{untyped({lowerIdx}Schema)}}")),
        expr_tuple_tag=", ".join(
            mkValsFmt(n, "{lowerIdx}Tag: c.Expr[TupleTag[{upperIdx}]]")
        ),
        expr_schema=", ".join(
            mkValsFmt(n, "{lowerIdx}Schema: c.Expr[Schema[{upperIdx}]]")
        ),
        assert_concrete="\n".join(mkValsFmt(n, "assertConcrete[{upperIdx}](c)")),
        tuple_tags=", ".join(mkValsFmt(n, "tupleTag(c)({lowerIdx}Tag)")),
        tag_trees=", ".join(mkValsFmt(n, "${lowerIdx}Tag")),
    )


def mkSQLBuilderFrom(n):
    return """
    private[sql] def from[{types}](q: String, {ref_args}, {tuple_tag_args}, udfs: List[Udf]): SQLBuilder = new SQLBuilder {{
        def as[R: Schema] =
        Sql
            .from({ref_colls})({ref_schemas})
            .queryAs(new Query{n}[{ref_types}, R](q, {tuple_tags}, udfs))
    }}
    """.format(
        n=n,
        types=mkTypes(n),
        ref_args=", ".join(mkValsFmt(n, "ref{upperIdx}: SCollectionRef[{upperIdx}]")),
        tuple_tag_args=mkTupleTagArgs(n),
        ref_colls=", ".join(mkValsFmt(n, "ref{upperIdx}.coll")),
        ref_schemas=", ".join(mkValsFmt(n, "ref{upperIdx}.schema")),
        ref_types=", ".join(mkValsFmt(n, "ref{upperIdx}._A")),
        tuple_tags=", ".join(mkValsFmt(n, "{lowerIdx}Tag")),
    )


def mkCases(n):
    return """
    case {ref_tuple_tags} :: Nil =>
        from[{ref_types}](q, {refs}, {tuple_tags}, udfs)
    """.format(
        refs=", ".join(mkValsFmt(n, "ref{upperIdx}")),
        ref_types=", ".join(mkValsFmt(n, "ref{upperIdx}._A")),
        tuple_tags=", ".join(mkValsFmt(n, "{lowerIdx}Tag")),
        ref_tuple_tags=" :: ".join(mkValsFmt(n, "(ref{upperIdx}, {lowerIdx}Tag)"))
    )


def mkSQLBuilder(n):
    return """
import com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.values.TupleTag

object SQLBuilders {{
    {from_methods}

  private[sql] def from(
    q: String,
    l: List[(SCollectionRef[Any], TupleTag[Any])],
    udfs: List[Udf]
  ): SQLBuilder =
    l match {{
      {cases}
      case ts =>
        throw new IllegalArgumentException(
          "sql interpolation only support JOIN on up to {n} unique " +
            s"SCollections, found ${{ts.length}}"
        )
    }}
}}
    """.format(
        n=n,
        from_methods="\n".join(mkSQLBuilderFrom(i) for i in xrange(1, n + 1)),
        cases="\n".join(mkCases(i) for i in xrange(1, n + 1)),
    )


header = textwrap.dedent(
    """
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

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! generated with sql.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

package com.spotify.scio.sql







"""
).lstrip("\n")


def main(out):

    N = 10
    for i in xrange(2, N + 1):
        f = open(
            "scio-core/src/main/scala/com/spotify/scio/sql/Query{}.scala".format(i), "w"
        )
        print >> f, header
        sqlCollectionFns(f, i)
        print >> f, textwrap.dedent(
            """
        
        
        
        
        
        
        """
        )
        f.close()
    f = open(
        "scio-core/src/main/scala/com/spotify/scio/sql/SqlSCollections.scala".format(i),
        "w",
    )
    print >> f, header
    print >> f, textwrap.dedent(
        """
        import com.spotify.scio.schemas._
        import com.spotify.scio.values.SCollection

        trait SqlSCollections {{
            {from_method}
        }}
        """.format(
            from_method="\n".join(mkFrom(n) for n in xrange(1, N + 1))
        )
    )
    print >> f, textwrap.dedent(
        """
        
        
        
        
        
        
        """
    )
    f.close()
    f = open(
    "scio-core/src/main/scala/com/spotify/scio/sql/SQLBuilders.scala".format(i),
    "w",
    )
    print >> f, header
    print >> f, mkSQLBuilder(N)
    print >> f, textwrap.dedent(
        """
        
        
        
        
        
        
        """
    )
    f.close()



if __name__ == "__main__":
    main(sys.stdout)
