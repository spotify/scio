# How to Release

## Prerequisites

- Sign up for a Sonatype account [here](https://issues.sonatype.org/secure/Signup!default.jspa)
- Ask for permissions to push to com.spotify domain like in this [ticket](https://issues.sonatype.org/browse/OSSRH-20689)
- Add Sonatype credentials to `~/.sbt/1.0/credentials.sbt`

```scala
credentials ++= Seq(
Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    "$USERNAME",
    "$PASSWORD"))
```
    
- Create a PGP key, for example on [keybase.io](https://keybase.io/), and [distribute](https://www.gnupg.org/gph/en/manual/x457.html) it to a public keyserver 

## Update version matrix

If the release includes a Beam version bump, update the @ref:[version matrix](../releases/Apache-Beam.md)

## Automatic (CI)

Checkout and update the `main` branch.

```bash
git checkout main

git pull
```

Create and push a new version tag

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"

git push origin vX.Y.Z
```

## Manual 

- Run the slow integration tests with `SLOW=true sbt it:test`
- Run `release skip-tests` in sbt console and follow the instructions
- Go to [oss.sonatype.org](https://oss.sonatype.org/), find the staging repository, "close" and "release"
- When the tag build completes, update release notes with name and change log
- Run `./scripts/make-site.sh` to update documentation

## After successfully published artifacts

- Clean the `mimaBinaryIssueFilters` in `build.sbt`
- Run @github[scripts/bump_scio.sh](/scripts/bump_scio.sh) to update [homebrew formula](https://github.com/spotify/homebrew-public/blob/master/scio.rb) and `scioVersion` in downstream repos including [scio.g8](https://github.com/spotify/scio.g8), [featran](https://github.com/spotify/featran), etc.
- Send external announcement to scio-users@googlegroups.com and user@beam.apache.org
- Announce on public [Slack](https://slackin.spotify.com/)
- Announce on Twitter
