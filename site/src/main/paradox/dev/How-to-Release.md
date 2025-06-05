# How to Release

## 1. Update version matrix

If the release includes a Beam version bump, update the @ref:[version matrix](../releases/Apache-Beam.md)

## 2. Run the pre-release check
Trigger the [pre-release check](https://github.com/spotify/scio/actions/workflows/pre-release-check.yml) and ensure that the latest CI build is green.

## 3. Release

The release steps are configured in [GHA](https://github.com/spotify/scio/blob/main/.github/workflows/ci.yml) using the [Typelevel CI Release plugin](https://typelevel.org/sbt-typelevel/customization.html#sbt-typelevel-sonatype-ci-release). Any tag pushed with the format `vx.y.z` will trigger a release of Scio verison `x.y.z`.

You can release fully using the GitHub UI, or manage tag creation on command line.

When drafting release notes, we recommend organizing the release notes into the following categories:

```md
## 🚀 Enhancements
## 🐛 Bug Fixes
## 📗 Documentation
## 🏗️ Build Improvements
## 🌱 Dependency Updates
```

Call out any Beam version upgrades in the top line of the release notes. See https://github.com/spotify/scio/releases for reference.

### Option 1: Release using GitHub UI

This is the easiest way to release the `main` branch. Simply draft a new release from the [Releases](https://github.com/spotify/scio/releases) tab and create a new tag (in the format `vx.y.z`). This will trigger a tag build in GHA, which performs the release steps automatically.

### Option 2: Release using Github CLI

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

In the GitHub UI, create a [GitHub release](https://github.com/spotify/scio/releases) from the newly created tag.

### Option 3: Release Manually

Included for reference, but not recommended.

**Prerequisites:**

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

**Manual release steps:**

- Run `release skip-tests` in sbt console and follow the instructions
- Go to [oss.sonatype.org](https://oss.sonatype.org/), find the staging repository, "close" and "release"
- When the tag build completes, draft a release in the GitHub UI using the existing tag. 

## 4. Post-release steps

When the artifacts have been published, you can:

- Clean the `mimaBinaryIssueFilters` in `build.sbt` if needed
- Run @github[scripts/bump_scio.sh](/scripts/bump_scio.sh) to update [homebrew formula](https://github.com/spotify/homebrew-public/blob/master/scio.rb) 
- Update `scioVersion` in downstream repos ([scio.g8](https://github.com/spotify/scio.g8), etc.)
- Send announcement to scio-users@googlegroups.com
