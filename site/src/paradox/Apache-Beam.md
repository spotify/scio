# Versions

Starting from version 0.3.0, Scio moved from Google Cloud [Dataflow Java SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) to [Apache Beam](https://beam.apache.org/) as its core dependencies and introduced a few breaking changes.

- Dataflow Java SDK 1.x.x uses `com.google.cloud.dataflow.sdk` namespace.
- Apache Beam uses `org.apache.beam.sdk` namespace.
- Dataflow Java SDK 2.x.x is also based on Apache Beam 2.x.x and uses `org.apache.beam.sdk`.

Scio 0.3.x depends on Beam 0.6.0 (last pre-stable release) and Scio 0.4.x depends on Beam 2.0.0 (first stable release). Breaking changes in these releases are documented below.

## Version matrices

Early Scio releases depend on Google Cloud [Dataflow Java SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) while later ones depend on [Apache Beam](https://github.com/apache/beam). Check out the [[Changelog]] page for migration guides.

Also check out the [SDK Version Support Status](https://cloud.google.com/dataflow/docs/support/sdk-version-support-status) page. Since a Beam release depends on specific version of Dataflow API, a deprecated Beam version is not guaranteed to work correctly or at all. We strongly recommend upgrading before the deprecation date.

| **Scio** | **SDK Dependency** | **Description**     |
|:--------:|:------------------:|:--------------------|
| 0.8.x    | Apache Beam 2.x.x  | Beam SQL, BigQuery storage API, ScioExecutionContext, Async `DoFn`s |
| 0.7.x    | Apache Beam 2.x.x  | Static coders, new ScioIO |
| 0.6.x    | Apache Beam 2.x.x  | Cassandra 2.2       |
| 0.5.x    | Apache Beam 2.x.x  | Better type-safe Avro and BigQuery IO |
| 0.4.x    | Apache Beam 2.0.0  | Stable release Beam |
| 0.3.x    | Apache Beam 0.6.0  | Pre-release Beam    |
| 0.2.x    | Dataflow Java SDK  | SQL-2011 support    |
| 0.1.x    | Dataflow Java SDK  | First releases      |

| **Scio Version** | **Beam Version** | **Details** |
|:----------------:|:----------------:|:------------|
| 0.8.0            | 2.17.0           | |
| 0.7.4            | 2.11.0           | This version will be deprecated on March 1, 2020. | 
| 0.7.3            | 2.10.0           | This version will be deprecated on February 11, 2020. |
| 0.7.2            | 2.10.0           | |
| 0.7.0+           | 2.9.0            | This version will be deprecated on December 13, 2019. |
| 0.6.0            | 2.6.0            | This version will be deprecated on August 8, 2019. |
| 0.5.7            | 2.6.0            | |
| 0.5.6            | 2.5.0            | This version will be deprecated on June 6, 2019. |
| 0.5.1+           | 2.4.0            | Deprecated as of March 20, 2019. |
| 0.5.0            | 2.2.0            | Deprecated as of December 2, 2018. |
| 0.4.6+           | 2.2.0            | |
| 0.4.1+           | 2.1.0            | Deprecated as of August 23, 2018. |
| 0.4.0            | 2.0.0            | Deprecated as of May 17, 2018. |
| 0.3.0+           | 0.6.0            | Unsupported |

## Release cycle and backport procedures

Scio has a frequent release cycle, roughly every 2-4 weeks, as compared to months for the upstream Apache Beam. We also aim to stay a step ahead by pulling changes from upstream and contributing new ones back.

Let's call the Beam version that Scio depends on `current`, and upstream master `latest`. Here're the procedures for backporting changes.

For changes available in `latest` but not in `current`:
- Copy Java files from `latest` to Scio repo
- Rename classes and modify as necessary
- Release Scio
- Update checklist for the next `current` version like @github[#633](#633)
- Remove change once `current` is updated

For changes we want to make to `latest`:
- Submit pull request to `latest`
- Follow the steps above once merged

## Beam master nightly build

To keep up with upstream changes, [beam-master](https://github.com/spotify/scio/tree/beam-master) branch is built nightly and depends on latest Beam SNAPSHOT.

We should do the following periodically to reduce work when upgrading Beam release version.
- rebase `beam-master` on `master`
- fix for breaking changes in `beam-master`
- rebase `master` on `beam-master` when upgrading Beam release version.

To work on a breaking change:
- checkout `beam-master` branch
- run `./scripts/circleci_snapshot.sh` to change `beamVersion` to the latest SNAPSHOT
- run `sbt test it:test` and fix errors
