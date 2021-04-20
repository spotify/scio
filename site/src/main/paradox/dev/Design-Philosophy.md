# Design Philosophy

We learned a lot building and improving Scio. The project was inspired by Spark and Scalding from the beginning, and we improved it over time working with customers of diverse background, including backend, data and ML. The design philosophy behind Scio can be summarized in a few points.

- **Make it easy to do the right thing**
  - Scala made this possible for the most part. We have a fluent API and it's easy to find the right transformation without going through lengthy documentation or source code. The most obvious thing is usually the best.
  - `.countByValue` is clearer and more efficient than `.map((_, 1L)).sumByKey` than `.map((_, 1L)).reduceByKey(_+_)`.
  - Case classes and `Option`s are much safer and easier than JSON-based `TableRow`s with `Object`s and nulls, despite the effort we went through to make it work.
  - One can `.sum` types with built-in `Semigroup`s easily and correctly.
  - Conversely there is no `.groupAll` since it could incur huge performance penalty and is essentially `.groupBy(_ => ())`. It's easier to ask than making the wrong assumption and use it wrong (_"because it's there"_).

- **Make common use cases simple**
  - We have syntactic sugar for most common IO modules e.g. `ScioContext#textFile`, `SCollection#saveAsBigQuery` but don't cover all possible parameters. There's a trade-off between covering more use cases and keeping the API simple.
  - We opted for a more flexible boilerplate free `Args` instead of the more type-safe `PipelineOptions` for command line arguments parsing.  Mistakes in these parts of the code are easier to catch and less damaging than those in the computation logic. Another trade-off we made.
  - We have syntactic sugars for various types of joins (hash, inner, outer, sketch) and side input operations (cross, lookup) that can be easily swapped to fine tune a pipeline.

- **Make complex use cases possible**
  - We wrap complex internal APIs but don't hide them away from users.
  - Most low level Beam APIs (`Pipeline`, `PCollection`, `PTransform`) are still easily accessible.
  - There are shorthands for integrating native Beam API, e.g. `ScioContext#customInput`, `SCollection#saveAsCustomOutput`, `SCollection#applyTransform`.
  - Pipelines can be submitted from `main`, another process, a backend service, or chained with `Future`s.
