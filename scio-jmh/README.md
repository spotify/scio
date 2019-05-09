# JMH benchmarks

JMH based benchmarks for certain specific components in Scio.

Getting started and running JMH benchmarks via sbt-shell:
```
$ sbt
...
sbt:scio> project scio-jmh
sbt:scio-jmh> jmh:run -f1 -wi 2 -i 3 .*BloomFilter.*Benchmark.*
```

The options for `jmh:run`
 - `-f1` Run with 1 fork
 - `-wi 2` Run 2 warm up iterations
 - `-i 3` Run 3 iterations
 - `.*BloomFilter.*Benchmark.*` RegExp for Benchmark
