Scio REPL
=========

# Getting started

The Scio REPL is an extension of the Scala REPL, with added functionality that allows you to
interactively experiment with Scio. Think of it as a playground to try out things.

# Quick start

The easiest way to start with Scio REPL is to assembly jar and run it:

```
$ sbt 'project scio-repl' assembly
$ java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar
Loading ...
System property 'bigquery.project' not set. BigQueryClient is not available.
Set it with '-Dbigquery.project=<PROJECT-NAME>' command line argument.
Scio context available as 'sc'
Welcome to Scio REPL!

scio>
```

Scio context is available as `sc`, this is the starting point, use `tab` completion, history and
all the other goods of REPL to play around. Ignore the BigQuery warning for now.

# Tutorial

Check full tutorial over [here](https://github.com/spotify/scio/wiki/Scio-REPL#local-pipeline)
