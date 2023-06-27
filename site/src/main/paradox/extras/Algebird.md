# Algebird

[Algebird](https://github.com/twitter/algebird) is Twitter's abstract algebra library. It has a lot of reusable modules for parallel aggregation and approximation. One can use any Algebird `Aggregator` or `Semigroup` with:
- `aggregate` and `sum` on @scaladoc[SCollection[T]](com.spotify.scio.values.SCollection)
- `aggregateByKey` and `sumByKey` on @scaladoc[SCollection[(K, V)]](com.spotify.scio.values.PairSCollectionFunctions)

See @github[AlgebirdSpec.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/AlgebirdSpec.scala) and [Algebird wiki](https://github.com/twitter/algebird/wiki) for more details. Also see these [slides](http://www.lyh.me/slides/semigroups.html) on semigroups.

### Algebird in REPL

```scala
scio> import com.twitter.algebird._
scio> import com.twitter.algebird.CMSHasherImplicits._
scio> val words = sc.textFile("README.md").
     | flatMap(_.split("[^a-zA-Z0-9]+")).
     | filter(_.nonEmpty).
     | aggregate(CMS.aggregator[String](0.001, 1E-10, 1)).
     | materialize
scio> sc.run()
scio> val cms = words.waitForResult().value.next
scio> cms.frequency("scio").estimate
res2: Long = 19

scio> // let's validate:
scio> import sys.process._
scio> "grep -o scio README.md"  #| "wc -l"!
      19
```
