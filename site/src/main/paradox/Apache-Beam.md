# Apache Beam Compatibility

Starting from version 0.3.0, Scio moved from Google Cloud [Dataflow Java SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) to [Apache Beam](https://beam.apache.org/) as its core dependencies and introduced a few breaking changes.

- Dataflow Java SDK 1.x.x uses `com.google.cloud.dataflow.sdk` namespace.
- Apache Beam uses `org.apache.beam.sdk` namespace.
- Dataflow Java SDK 2.x.x is also based on Apache Beam 2.x.x and uses `org.apache.beam.sdk`.

Scio 0.3.x depends on Beam 0.6.0 (last pre-stable release) and Scio 0.4.x depends on Beam 2.0.0 (first stable release). Breaking changes in these releases are documented below.

## Version matrices

Early Scio releases depend on Google Cloud [Dataflow Java SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK) while later ones depend on [Apache Beam](https://github.com/apache/beam). Check out the [[Changelog]] page for migration guides.

Also check out the [SDK Version Support Status](https://cloud.google.com/dataflow/docs/support/sdk-version-support-status) page. Since a Beam release depends on specific version of Dataflow API, a deprecated Beam version is not guaranteed to work correctly or at all. We strongly recommend upgrading before the deprecation date.

| **Scio** | **SDK Dependency** | **Description**                                                                                                                     |
|:--------:|:------------------:|:------------------------------------------------------------------------------------------------------------------------------------|
|  0.12.x  | Apache Beam 2.x.x  | com.spotify.scio.extra.bigquery, com.spotify.scio.pubsub removed. scio-elasticsearch6 deprecated.                                   |
|  0.11.x  | Apache Beam 2.x.x  | scio-sql and case-app removed, com.spotify.scio.extra.bigquery deprecated, shaded Beam Avro coder, `tensorflow-core-platform` 0.3.3 |
|  0.10.x  | Apache Beam 2.x.x  | Coder implicits, `scio-google-cloud-platform`                                                                                       |
|  0.9.x   | Apache Beam 2.x.x  | Drop Scala 2.11, add Scala 2.13, Guava based Bloom Filter                                                                           |
|  0.8.x   | Apache Beam 2.x.x  | Beam SQL, BigQuery storage API, ScioExecutionContext, Async `DoFn`s                                                                 |
|  0.7.x   | Apache Beam 2.x.x  | Static coders, new ScioIO                                                                                                           |
|  0.6.x   | Apache Beam 2.x.x  | Cassandra 2.2                                                                                                                       |
|  0.5.x   | Apache Beam 2.x.x  | Better type-safe Avro and BigQuery IO                                                                                               |
|  0.4.x   | Apache Beam 2.0.0  | Stable release Beam                                                                                                                 |
|  0.3.x   | Apache Beam 0.6.0  | Pre-release Beam                                                                                                                    |
|  0.2.x   | Dataflow Java SDK  | SQL-2011 support                                                                                                                    |
|  0.1.x   | Dataflow Java SDK  | First releases                                                                                                                      |

| **Scio Version** | **Beam Version** | **Details**                                           |
|:----------------:|:----------------:|:------------------------------------------------------|
|      0.12.1      |      2.43.0      | This version will be deprecated on November 17, 2023. |
|      0.12.0      |      2.41.0      | This version will be deprecated on August 23rd, 2023. |
|     0.11.13      |      2.41.0      | This version will be deprecated on August 23rd, 2023. |
|     0.11.12      |      2.41.0      | This version will be deprecated on August 23rd, 2023. |
|     0.11.11      |      2.41.0      | This version will be deprecated on August 23rd, 2023. |
|     0.11.10      |      2.41.0      | This version will be deprecated on August 23rd, 2023. |
|      0.11.9      |      2.39.0      | This version will be deprecated on May 25, 2023.      |
|      0.11.6      |      2.38.0      | This version will be deprecated on April 20, 2023.    |
|      0.11.5      |      2.36.0      | This version will be deprecated on February 7, 2023.  |
|      0.11.4      |      2.35.0      | This version will be deprecated on December 29, 2022. |
|      0.11.3      |      2.35.0      | This version will be deprecated on December 29, 2022. |
|      0.11.2      |      2.34.0      | This version will be deprecated on November 11, 2022. |
|      0.11.1      |      2.33.0      | This version will be deprecated on October 7, 2022.   |
|      0.11.0      |      2.32.0      | This version will be deprecated on August 25, 2022.   |
|     0.10.4+      |      2.30.0      | This version will be deprecated on June 10, 2022.     |
|      0.10.3      |      2.29.0      | Deprecated as of April 29, 2022.                      |
|     0.10.0+      |      2.28.0      | Deprecated as of February 22, 2022.                   |
|      0.9.5+      |      2.24.0      | Deprecated as of September 18, 2021.                  |
|      0.9.3+      |      2.23.0      | Deprecated as of July 29, 2021.                       |
|      0.9.2       |      2.22.0      | Deprecated as of June 8, 2021.                        |
|      0.9.1       |      2.20.0      | Deprecated as of April 15, 2021.                      |
|      0.9.0       |      2.20.0      |                                                       |
|      0.8.2+      |      2.19.0      | Deprecated as of February 4, 2021.                    |
|      0.8.1       |      2.18.0      | Deprecated as of January 23, 2021.                    |
|      0.8.0       |      2.17.0      | Deprecated as of January 6, 2021.                     |
|      0.7.4       |      2.11.0      | Deprecated as of March 1, 2020.                       |
|      0.7.3       |      2.10.0      | Deprecated as of February 11, 2020.                   |
|      0.7.2       |      2.10.0      |                                                       |
|      0.7.0+      |      2.9.0       | Deprecated as of December 13, 2019.                   |
|      0.6.0       |      2.6.0       | Deprecated as of August 8, 2019.                      |
|      0.5.7       |      2.6.0       |                                                       |
|      0.5.6       |      2.5.0       | Deprecated as of June 6, 2019.                        |
|      0.5.1+      |      2.4.0       | Deprecated as of March 20, 2019.                      |
|      0.5.0       |      2.2.0       | Deprecated as of December 2, 2018.                    |
|      0.4.6+      |      2.2.0       |                                                       |
|      0.4.1+      |      2.1.0       | Deprecated as of August 23, 2018.                     |
|      0.4.0       |      2.0.0       | Deprecated as of May 17, 2018.                        |
|      0.3.0+      |      0.6.0       | Unsupported                                           |

## Beam dependencies

Scio's other library dependencies are kept in sync with Beam's to avoid compatibility issues. You can find
Beam's dependency list in its [Groovy config](https://github.com/apache/beam/blob/v2.35.0/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy) (substitute the version tag in the URL with the desired Beam version). Additionally, Beam keeps many of its Google dependencies in sync with a [central BOM](https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/24.0.0/artifact_details.html) (subsitute the version tag in the URL with the value of `google_cloud_platform_libraries_bom` from Beam). Scio users who suspect incompatibility issues in their pipelines (common issues are GRPC, Netty, or Guava) can run `sbt evicted` and `sbt dependencyTree` to ensure their direct and transitive dependencies don't conflict with Scio or Beam.

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
