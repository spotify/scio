# Testing

To write Scio unit tests you will need to add the following dependency to your build.sbt

```scala
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
```scala
 val inData = Seq("a b c d e", "a b a b", "")
 val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")
```

Using `JobTest`, you can test the entire pipeline. Specify the type of the class under test, in this case it is com.spotify.scio.examples.WordCount.type . The `args` function takes the list of command line arguments passed to the main function of WordCount.  The WordCount’s main function expects input and output arguments passed to it.

```scala
"WordCount" should "work" in {
 JobTest[com.spotify.scio.examples.WordCount.type]
   .args("--input=in.txt", "--output=out.txt")
   .input(TextIO("in.txt"), inData)
   .output(TextIO("out.txt")){ coll =>
coll should containInAnyOrder(expected)
()
}
   .run()
}
```

The `input` function injects your input test data. Note that the `TestIO[T]` should match the input source used in the pipeline e.g. TextIO for sc.textFile, AvroIO for sc.avro. The TextIO id (“in.txt”) should match the one specified in the args.

The output function evaluates the output of the pipeline using the provided assertion from the `SCollectionMatchers`. More info on `SCollectionMatchers` can be found [here](https://spotify.github.io/scio/api/com/spotify/scio/testing/SCollectionMatchers.html). In this example, we are asserting that the output of the pipeline should contain an `SCollection` with elements that in the expected variable in any order.
Also, note that the `TestIO[T]` should match the output used in the pipeline e.g. TextIO for sc.saveAsTextFile

The run function will run the pipeline.

### Test for pipeline with sideinput
We will use the @github[SideInputJoinExamples](/scio-examples/src/test/scala/com/spotify/scio/examples/cookbook/JoinExamplesTest.scala#L73) test in JoinExamplesTest to illustrate how to write a test for pipelines with sideinputs. The @extref[SideInputJoinExamples](example:JoinExamples) pipeline has two input sources, one for eventsInfo and the other for countryInfo. CountryInfo is used as a sideinput to join with eventInfo.

Since we have two input sources, we have to specify both in the `JobTest`. Note that the injected data type should match one expected by the sink.

```scala
"SideInputJoinExamples" should "work" in {
 JobTest[com.spotify.scio.examples.cookbook.SideInputJoinExamples.type]
   .args("--output=out.txt")
   .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
   .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
   .output(TextIO("out.txt")){ coll =>
coll should containInAnyOrder(expected)
()
}
   .run()
}
```
### Test for pipeline with sideoutput
@github[SideInOutExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/SideInOutExampleTest.scala) shows an example of how to test pipelines with sideoutputs. Each sideoutput is evaluated using the output function. The ids for `TextIO`  e.g. “out1.txt” should match the ones specified in the args.

```scala
"SideInOutExample" should "work" in {
  JobTest[SideInOutExample.type]
    .args("--input=in.txt",
        "--stopWords=stop.txt",
        "--output1=out1.txt",
        "--output2=out2.txt",
        "--output3=out3.txt",
        "--output4=out4.txt")
    .output(TextIO("out1.txt")){ coll =>
      coll should containInAnyOrder(Seq.empty[String])
      ()
    }
    .output(TextIO("out2.txt")){ coll =>
      coll should containInAnyOrder(Seq.empty[String])
      ()
    }.run()
}
```

### Test partial pipeline
To test a section of a pipeline, use `runWithContext`. The TriggerExample.extractFlowInfo test in @github[TriggerExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/cookbook/TriggerExampleTest.scala) tests only the extractFlowInfo part of the pipeline.

The data variable hold the test data and `sc.parallelize` will transform the input iterable to a `SCollection` of strings. TriggerExample.extractFlowInfo will be executed using the `ScioContext` and you can then specify assertions against the result of the pipeline.

```scala
val data = Seq(
 "01/01/2010 00:00:00,1108302,94,E,ML,36,100,29,0.0065,66,9,1,0.001,74.8,1,9,3,0.0028,71,1,9,"
   + "12,0.0099,67.4,1,9,13,0.0121,99.0,1,,,,,0,,,,,0,,,,,0,,,,,0",
 "01/01/2010 00:00:00,"
   + "1100333,5,N,FR,9,0,39,,,9,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,"
)

runWithContext { sc =>
  val r = TriggerExample.extractFlowInfo(sc.parallelize(data))
  r should haveSize(1)
  r should containSingleValue(("94", 29))
}
```

### Test for pipeline with windowing
We will use the LeaderBoardTest to explain how to test Windowing in Scio. The full example code is found @github[here](/scio-examples/src/test/scala/com/spotify/scio/examples/complete/game/LeaderBoardTest.scala). LeaderBoardTest also extends `PipelineSpec`. The function under test is the @github[LeaderBoard.calculateTeamScores](/scio-examples/src/main/scala/com/spotify/scio/examples/complete/game/LeaderBoard.scala#L131).  This function calculates teams scores within a fixed window with the following the window options:
* Calculate the scores every time the window ends
* Calculate an early/"speculative" result from partial data, 5 minutes after the first element in our window is processed (withEarlyFiring)
* Accept late entries (and recalculates based on them) only if they arrive within the allowedLateness duration.

In this test,  we are testing calculateTeamScores for when all of the elements arrive on time, i.e. before the watermark.

We specify, the fixed window size is set to 20 minutes and allowedLateness is 1 hour.
private val allowedLateness = Duration.standardHours(1)
private val teamWindowDuration = Duration.standardMinutes(20)

First, we have to create an input stream representing an unbounded `SCollection` of type GameActionInfo using the `testStreamOf`. Each element is assigned a timestamp representing when each event occurred. In the code snippet above, we start at epoch equal zero,  by setting watermark to 0 in the advanceWatermarkTo.

We add GameActionInfo elements with varying timestamps, and we advanced the watermark to 3 minutes. At this point, all elements are on time because they came before the watermark advances to 3 minutes.

```scala
val stream = testStreamOf[GameActionInfo]
// Start at the epoch
 .advanceWatermarkTo(baseTime)
 // add some elements ahead of the watermark
 .addElements(
   event(blueOne, 3, Duration.standardSeconds(3)),
   event(blueOne, 2, Duration.standardMinutes(1)),
   event(redTwo, 3, Duration.standardSeconds(22)),
   event(blueTwo, 5, Duration.standardSeconds(3))
 )
 // The watermark advances slightly, but not past the end of the window
 .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
```
We then more GameActionInfo elements and advance the watermark to infinity by calling the advanceWatermarkToInfinity. Similarly, these elements are also on time because the watermark is infinity.

```scala
.addElements(event(redOne, 1, Duration.standardMinutes(4)),
            event(blueOne, 2, Duration.standardSeconds(270)))
// The window should close and emit an ON_TIME pane
.advanceWatermarkToInfinity
```

To run the test, we use the `runWithContext`, this will run calculateTeamScores using the ScioContext. In calculateTeamScores, we pass the `SCollection` we created above using `testStreamOf`. The `IntervalWindow` specifies the window for which we want to assert the `SCollection` of elements created by calculateTeamScores. We want to assert that elements with initial window of 0 to 20 minutes were on time. Next we assert, using `inOnTimePane` that the `SCollection` elements are equal to the expected sums.

```scala
runWithContext { sc =>
 val teamScores =
   LeaderBoard.calculateTeamScores(sc.testStream(stream), teamWindowDuration, allowedLateness)

 val window = new IntervalWindow(baseTime, teamWindowDuration)
 teamScores should inOnTimePane(window) {
   containInAnyOrder(Seq((blueOne.team, 12), (redOne.team, 4)))
 }
}
```
Scio provides more `SCollection` assertions such as `inWindow`, `inCombinedNonLatePanes`, `inFinalPane`, and `inOnlyPane`. You can find the full list [here](https://spotify.github.io/scio/api/com/spotify/scio/testing/SCollectionMatchers.html). More information on testing unbounded pipelines can be found [here](https://beam.apache.org/blog/2016/10/20/test-stream.html).




