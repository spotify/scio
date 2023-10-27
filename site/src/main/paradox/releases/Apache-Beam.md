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
|  0.13.x  | Apache Beam 2.x.x  | scio-elasticsearch6 removed. scio-elasticsearch7 migrated to new client. File based ScioIO param changes.                           |
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
|      0.13.5      |      2.51.0      | This version will be deprecated on October 12, 2024.  |
|      0.13.4      |      2.51.0      | This version will be deprecated on October 12, 2024.  |
|      0.13.3      |      2.50.0      | This version will be deprecated on August 30, 2024.   |
|      0.13.2      |      2.49.0      | This version will be deprecated on July 17, 2024.     |
|      0.13.1      |      2.49.0      | This version will be deprecated on July 17, 2024.     |
|      0.13.0      |      2.48.0      | This version will be deprecated on May 31, 2024.      |
|      0.12.8      |      2.46.0      | This version will be deprecated on March 10, 2024.    |
|      0.12.7      |      2.46.0      | This version will be deprecated on March 10, 2024.    |
|      0.12.6      |      2.46.0      | This version will be deprecated on March 10, 2024.    |
|      0.12.5      |      2.45.0      | This version will be deprecated on February 15, 2024. |
|      0.12.4      |      2.44.0      | This version will be deprecated on January 13, 2024.  |
|      0.12.3      |      2.44.0      | This version will be deprecated on January 13, 2024.  |
|      0.12.2      |      2.44.0      | This version will be deprecated on January 13, 2024.  |
|      0.12.1      |      2.43.0      | This version will be deprecated on November 17, 2023. |
|      0.12.0      |      2.41.0      | Deprecated on August 23rd, 2023.                      |
|     0.11.14      |      2.41.0      | Deprecated on August 23rd, 2023.                      |
|     0.11.13      |      2.41.0      | Deprecated on August 23rd, 2023.                      |
|     0.11.12      |      2.41.0      | Deprecated on August 23rd, 2023.                      |
|     0.11.11      |      2.41.0      | Deprecated on August 23rd, 2023.                      |
|     0.11.10      |      2.41.0      | Deprecated on August 23rd, 2023.                      |
|      0.11.9      |      2.39.0      | Deprecated on May 25, 2023.                           |
|      0.11.6      |      2.38.0      | Deprecated on April 20, 2023.                         |
|      0.11.5      |      2.36.0      | Deprecated on February 7, 2023.                       |
|      0.11.4      |      2.35.0      | Deprecated on December 29, 2022.                      |
|      0.11.3      |      2.35.0      | Deprecated on December 29, 2022.                      |
|      0.11.2      |      2.34.0      | Deprecated on November 11, 2022.                      |
|      0.11.1      |      2.33.0      | Deprecated on October 7, 2022.                        |
|      0.11.0      |      2.32.0      | Deprecated on August 25, 2022.                        |

## Beam dependencies

Scio's other library dependencies are kept in sync with Beam's to avoid compatibility issues. Scio will typically _not_ bump dependency versions beyond what is supported in Beam due to the large test surface and the potential for data loss.

You can find Beam's dependency list in its [Groovy config](https://github.com/apache/beam/blob/v2.35.0/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy) (substitute the version tag in the URL with the desired Beam version). Additionally, Beam keeps many of its Google dependencies in sync with a [central BOM](https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/24.0.0/artifact_details.html) (substitute the version tag in the URL with the value of `google_cloud_platform_libraries_bom` from Beam). Scio users who suspect incompatibility issues in their pipelines (common issues are GRPC, Netty, or Guava) can run `sbt evicted` and `sbt dependencyTree` to ensure their direct and transitive dependencies don't conflict with Scio or Beam.

## Release cycle and backport procedures

Scio has a frequent release cycle, roughly every 2-4 weeks, as compared to months for the upstream Apache Beam. We also aim to stay a step ahead by pulling changes from upstream and contributing new ones back.

Let's call the Beam version that Scio depends on `current`, and upstream master `latest`. Here are the procedures for backporting changes:

For changes available in `latest` but not in `current`:
- Copy Java files from `latest` to Scio repo
- Rename classes and modify as necessary
- Release Scio
- Update checklist for the next `current` version like @github[#633](#633)
- Remove change once `current` is updated

For changes we want to make to `latest`:
- Submit pull request to `latest`
- Follow the steps above once merged
