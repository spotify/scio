# AsyncDoFn

Scio's @scaladoc[BaseAsyncDoFn](com.spotify.scio.transforms.BaseAsyncDoFn) provides standard handling for sending asynchronous requests and capturing the responses for a bundle of pipeline elements.
`BaseAsyncDoFn` is a subclass of @scaladoc[DoFnWithResource](com.spotify.scio.transforms.DoFnWithResource) which handles the creation and re-use of client classes.
Scio provides several future-specific subclasses to choose from depending on the return type of the client:

* @scaladoc[GuavaAsyncDoFn](com.spotify.scio.transforms.GuavaAsyncDoFn) for clients that return Guava's `ListenableFuture`
* @scaladoc[JavaAsyncDoFn](com.spotify.scio.transforms.JavaAsyncDoFn) for clients that return `CompletableFuture`
* @scaladoc[ScalaAsyncDoFn](com.spotify.scio.transforms.ScalaAsyncDoFn) for clients that return a scala `Future`

`BaseAsyncDoFn` will wait for all futures for all bundle elements to be returned before completing the bundle.
A failure of any request for an item in the bundle will cause the entire bundle to be retried.
Requests should therefore be idempotent.

Given this Guava-based mock client:
```scala
import com.google.common.util.concurrent.{ListenableFuture, Futures}

case class MyClient(value: String) {
  def request(i: Int): ListenableFuture[String] = Futures.immediateFuture(s"$value$i")
}
```

For client which returns a `ListenableFuture`, a custom `DoFn` can be defined using `GuavaAsyncDoFn`.
Note the configured `ResourceType`, which will re-use the client for all threads on a worker, see @scaladoc[ResourceType](com.spotify.scio.transforms.DoFnWithResource.ResourceType) for more details.

```scala
import com.spotify.scio.transforms._
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.ParDo

class MyDoFn() extends GuavaAsyncDoFn[Int, String, MyClient] {
  override def getResourceType: ResourceType = ResourceType.PER_CLASS
  override def createResource(): MyClient = MyClient("foo")
  override def processElement(input: Int): ListenableFuture[String] =
    getResource.request(input)
}

val elements: SCollection[Int] = ???
val result: SCollection[String] = elements.applyTransform(ParDo.of(new MyDoFn()))
```
