# Testing

To write Scio unit tests you will need to add the following dependency to your build.sbt

```sbt
libraryDependencies ++= Seq(
  // .......
  "com.spotify" %% "scio-test" % scioVersion % Test,
  // .......
)
```
To run the test, you can run the following commands. You can skip the first two lines if you already ran them and are still in the sbt shell.

```cmd
$ sbt
> project scio-examples
> test
```
Click on this [link](https://www.scala-sbt.org/1.x/docs/Testing.html) for more Scala testing tasks.

### Test entire pipeline
We will use the WordCountTest to explain how Scio tests work. @extref[WordCount](example:WordCount) is the pipeline under test. Full example code for WordCountTest and other test examples can be found @github[here](/scio-examples/src/test).

Let’s walk through the details of the test: The test class should extend the `PipelineSpec` which is a trait for unit testing pipelines.

The inData variable holds the input data for your test and the expected variable contains the expected results after your pipeline processes the inData. The WordCount pipeline counts the occurrence of each word, the given the input data , we should expect a count of a=3, b=3, c=1 etc
```scala mdoc
 val inData = Seq("a b c d e", "a b a b", "")
 val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")
```

Using `JobTest`, you can test the entire pipeline. Specify the type of the class under test, in this case it is com.spotify.scio.examples.WordCount.type . The `args` function takes the list of command line arguments passed to the main function of WordCount.  The WordCount’s main function expects input and output arguments passed to it.

@@snip [WordCountTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/WordCountTest.scala) { #WordCountTest_example }

The `input` function injects your input test data. Note that the `TestIO[T]` should match the input source used in the pipeline e.g. TextIO for sc.textFile, AvroIO for sc.avro. The TextIO id (“in.txt”) should match the one specified in the args.

The output function evaluates the output of the pipeline using the provided assertion from the `SCollectionMatchers`. More info on `SCollectionMatchers` can be found [here](https://spotify.github.io/scio/api/com/spotify/scio/testing/SCollectionMatchers.html). In this example, we are asserting that the output of the pipeline should contain an `SCollection` with elements that in the expected variable in any order.
Also, note that the `TestIO[T]` should match the output used in the pipeline e.g. TextIO for sc.saveAsTextFile

The run function will run the pipeline.

### Test for pipeline with sideinput
We will use the @github[SideInputJoinExamples](/scio-examples/src/test/scala/com/spotify/scio/examples/cookbook/JoinExamplesTest.scala#L73) test in JoinExamplesTest to illustrate how to write a test for pipelines with sideinputs. The @extref[SideInputJoinExamples](example:JoinExamples) pipeline has two input sources, one for eventsInfo and the other for countryInfo. CountryInfo is used as a sideinput to join with eventInfo.

Since we have two input sources, we have to specify both in the `JobTest`. Note that the injected data type should match one expected by the sink.

@@snip [JoinExampleTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/cookbook/JoinExamplesTest.scala) { #JoinExamplesTest_example }

### Test for pipeline with sideoutput
@github[SideInOutExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/SideInOutExampleTest.scala) shows an example of how to test pipelines with sideoutputs. Each sideoutput is evaluated using the output function. The ids for `TextIO`  e.g. “out1.txt” should match the ones specified in the args.

@@snip [SideInOutExampleTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/SideInOutExampleTest.scala) { #SideInOutExampleTest_example}

### Test partial pipeline
To test a section of a pipeline, use `runWithContext`. The TriggerExample.extractFlowInfo test in @github[TriggerExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/cookbook/TriggerExampleTest.scala) tests only the extractFlowInfo part of the pipeline.

The data variable hold the test data and `sc.parallelize` will transform the input iterable to a `SCollection` of strings. TriggerExample.extractFlowInfo will be executed using the `ScioContext` and you can then specify assertions against the result of the pipeline.

@@snip [TriggerExampleTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/cookbook/TriggerExampleTest.scala) { #TriggerExampleTest_example }

### Test for pipeline with windowing
We will use the LeaderBoardTest to explain how to test Windowing in Scio. The full example code is found @github[here](/scio-examples/src/test/scala/com/spotify/scio/examples/complete/game/LeaderBoardTest.scala). LeaderBoardTest also extends `PipelineSpec`. The function under test is the @github[LeaderBoard.calculateTeamScores](/scio-examples/src/main/scala/com/spotify/scio/examples/complete/game/LeaderBoard.scala#L131).  This function calculates teams scores within a fixed window with the following the window options:
* Calculate the scores every time the window ends
* Calculate an early/"speculative" result from partial data, 5 minutes after the first element in our window is processed (withEarlyFiring)
* Accept late entries (and recalculates based on them) only if they arrive within the allowedLateness duration.

In this test,  we are testing calculateTeamScores for when all of the elements arrive on time, i.e. before the watermark.

First, we have to create an input stream representing an unbounded `SCollection` of type GameActionInfo using the `testStreamOf`. Each element is assigned a timestamp representing when each event occurred. In the code snippet above, we start at epoch equal zero,  by setting watermark to 0 in the advanceWatermarkTo.

We add GameActionInfo elements with varying timestamps, and we advanced the watermark to 3 minutes. At this point, all elements are on time because they came before the watermark advances to 3 minutes.

@@snip [LeaderBoardTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/complete/game/LeaderBoardTest.scala) { #LeaderBoardTest_example_1 }

We then more GameActionInfo elements and advance the watermark to infinity by calling the advanceWatermarkToInfinity. Similarly, these elements are also on time because the watermark is infinity.

@@snip [LeaderBoardTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/complete/game/LeaderBoardTest.scala) { #LeaderBoardTest_example_2 }

To run the test, we use the `runWithContext`, this will run calculateTeamScores using the ScioContext. In calculateTeamScores, we pass the `SCollection` we created above using `testStreamOf`. The `IntervalWindow` specifies the window for which we want to assert the `SCollection` of elements created by calculateTeamScores. We want to assert that elements with initial window of 0 to 20 minutes were on time. Next we assert, using `inOnTimePane` that the `SCollection` elements are equal to the expected sums.

@@snip [LeaderBoardTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/complete/game/LeaderBoardTest.scala) { #LeaderBoardTest_example_3 }

Scio provides more `SCollection` assertions such as `inWindow`, `inCombinedNonLatePanes`, `inFinalPane`, and `inOnlyPane`. You can find the full list [here](https://spotify.github.io/scio/api/com/spotify/scio/testing/SCollectionMatchers.html). More information on testing unbounded pipelines can be found [here](https://beam.apache.org/blog/2016/10/20/test-stream.html).




