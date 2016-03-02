Scio REPL
=========

# Getting started

The Scio REPL is an extension of the Scala REPL, with added functionality that allows you to
interactively experiment with Scio. Think of it as a playground to try out things.

# Quick start

The easiest way to start with Scio REPL is to assembly jar and run it:

```bash
$ sbt 'project scio-repl' assembly
$ java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar
Starting up ...
Scio context is available at 'sc'
Welcome to Scio REPL!
scio>
```
Scio context is available as `sc`, this is the starting point, use `tab` completion, history and
all the other goods of REPL to play around in local mode.

# Tutorial

Create new Scio context in REPL:

```
scio> :newScio myContext
Scio context is available at 'myContext'
scio> myContext.parallelize(List(1,2,3))
res1: com.spotify.scio.values.SCollection[Int] = com.spotify.scio.values.SCollectionImpl@5ab7e997
```

To create Scio context connected to Google's Dataflow service pass Scio arguments on REPL startup -
each `:newScio` will use those arguments for new Scio contexts. For example:

```bash
$ java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=BlockingDataflowPipelineRunner
Starting up ...
Scio context is available at 'sc'
Welcome to Scio REPL!
scio> sc.parallelize(List(1,2,3)).map( _.toString ).saveAsTextFile("gs://<output-dir>")
scio> val result = sc.close
```

At any point in time you can always create a local Scio context using `:newLocalScio <name>`:

```
scio> :newLocalScio lsc
Local Scio context is available at 'lsc'
```

# Tips

## Use `:paste` for multiline code

While in the REPL, use `:paste` magic command to past/write multi line code

```
scio> :paste
// Entering paste mode (ctrl-D to finish)
def myWordFilter(word: String) = word.startsWith("rav")
val importantNames = sc.parallelize
parallelize   parallelizeTimestamped
val importantNames = sc.parallelize(List("ravioli", "rav", "jbx")).filter(myWordFilter)
// Exiting paste mode, now interpreting.
myWordFilter: (word: String)Boolean
importantNames: com.spotify.scio.values.SCollection[String] = com.spotify.scio.values.SCollectionImpl@75882ac2
scio> importantNames.saveAsTextFile("/tmp/ravs")
scio> sc.close
```

