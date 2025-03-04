# Scala Steward

Scio uses [Scala Steward](https://github.com/scala-steward-org/scala-steward) to keep non-managed dependencies up to date.
Our repo-specific configuration, which excludes dependencies synchronized with Beam, is defined in @github[.scala-steward.conf](/.scala-steward.conf).

## Running Scala Steward locally

Sometimes, the remote Scala Steward instance fails to open PRs for us, usually due to [buffer overflow](https://github.com/scala-steward-org/scala-steward/issues/2718).
In this case, it can be run manually, which will open PRs using your personal credentials.

To run Scala Steward locally, you should have both https://github.com/scala-steward-org/scala-steward and https://github.com/spotify/scio cloned locally.

```shell
$ # replace with your local directory paths
$ SCALA_STEWARD_DIR=/workspace/scala-steward
$ SCIO_DIR=/workspace/scio

$ echo "- spotify/scio" > $SCALA_STEWARD_DIR/repos.md

$ cd $SCALA_STEWARD_DIR
$ sbt stage
$ ./modules/core/.jvm/target/universal/stage/bin/scala-steward \
  --workspace  "$SCALA_STEWARD_DIR/workspace" \
  --repos-file "$SCALA_STEWARD_DIR/repos.md" \
  --repo-config "$SCIO_DIR/.scala-steward.conf" \
  --git-author-email <YOUR_EMAIL_HERE> \
  --forge-api-host "https://api.github.com" \
  --forge-login <YOUR_EMAIL_HERE> \
  --do-not-fork \
  --max-buffer-size 64000 \
  --git-ask-pass "$SCALA_STEWARD_DIR/ask_pass.sh"
```

You'll have to add the file `$SCALA_STEWARD_DIR/ask_pass.sh`, which should echo your personal [Github Acccess Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).
The token must have `public_repo` permissions to execute Scala Steward.

```shell
#!/bin/sh
echo "<MY_GITHUB_ACCESS_TOKEN>"
```

For more information on running Scala Steward locally, check the [reference guide](https://github.com/scala-steward-org/scala-steward/blob/main/docs/running.md).
