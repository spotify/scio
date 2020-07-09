Contributing
============

Scio is developed and maintained by an infrastructure team at Spotify. It is the preferred API for both batch and streaming data processing on Google Cloud Dataflow at Spotify and many other companies. The project wouldn't have been successful without contributions from all the external users.

# Submitting issues

Feel free to discuss issues in the #scio channel on Spotify FOSS Slack (get an invite [here](https://slackin.spotify.com/)) or [Google Group](https://groups.google.com/forum/#!forum/scio-users) first.

Don't hesitate to create [GitHub issues](https://github.com/spotify/scio/issues) for bugs, feature requests, or questions. When reporting a bug, it would help to include a small, reproducible code snippet or unit test.

# Submitting Pull Requests

Before opening a pull request, make sure `sbt test` runs successfully and that your code is properly formatted with  `sbt scalafmtAll javafmtAll`. It's usually a good idea to keep changes simple and small. Please also be consistent with the code base and our [style guide](https://spotify.github.io/scio/dev/Style-Guide.html).

If there is already a GitHub issue for the task you are working on, leave a comment to let people know that you are working on it. If there isn't already an issue and it is a non-trivial task, it's a good idea to create one (and note that you're working on it). This prevents contributors from duplicating effort.

# Contributing documentation

You can contribute to Scio documentation, and the API documentation.

Run [scripts/make-site.sh](scripts/make-site.sh) in the project root. The generated site is under `site/target/site/index.html`.

The examples in the markdown documentation are built using [mdoc](https://scalameta.org/mdoc/).
While you're writing documentation, you can check that everything compiles by running `mdoc` in the sbt shell.
You can also run `~mdoc` to automatically build the documentation on save.

# Building locally

See [this page](https://spotify.github.io/scio/dev/build.html) for building Scio locally.

# Code of Conduct

This project adheres to the [Spotify FOSS Code of Conduct][code-of-conduct]. By participating, you are expected to honor this code.

[code-of-conduct]: https://github.com/spotify/scio/blob/master/CODE_OF_CONDUCT.md
