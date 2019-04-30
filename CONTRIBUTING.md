Contributing
============

Scio is developed and maintained by an infrastructure team at Spotify. It is the preferred API for both batch and streaming data processing on Google Cloud Dataflow at Spotify and many other companies. The project wouldn't have been successful without contributions from all the external users.

# Submitting issues

Feel free to discuss issues in our [Gitter room](https://gitter.im/spotify/scio) or [Google Group](https://groups.google.com/forum/#!forum/scio-users) first.

Don't hesitate to create [GitHub issues](https://github.com/spotify/scio/issues) for bugs, feature requests, or questions. When reporting a bug, it would help to include a small, reproducible code snippet or unit test.

# Submitting Pull Requests

Before opening a pull request, make sure `sbt scalastyle test` runs successfully. You can also format your code automatically with `sbt scalafmt test:scalafmt scalafmtSbt`. It's usually a good idea to keep changes simple and small. Please also be consistent with the code base and our [style guide](https://spotify.github.io/scio/dev/Style-Guide.html).

If there is already a GitHub issue for the task you are working on, leave a comment to let people know that you are working on it. If there isn't already an issue and it is a non-trivial task, it's a good idea to create one (and note that you're working on it). This prevents contributors from duplicating effort.

# Contributing documentation

You can contribute to Scio documentation, and the API documentation.

Run `SOCCO=true sbt clean scio-examples/compile site/makeSite` in the project root. The generated site is under `site/target/site/index.html`.

The examples in the markdown documentation are built using [mdoc](https://scalameta.org/mdoc/).
While you're writing documentation, you can check that everything compiles by running `mdoc` in the sbt shell.
You can also run `~mdoc` to automatically build the documentation on save.

# Code of Conduct

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are expected to honor this code.

[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
