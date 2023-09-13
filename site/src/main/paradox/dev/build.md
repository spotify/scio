# Build

## Getting the source

```bash
git clone https://github.com/spotify/scio.git
```

## Compiling

Build and test the code.

```bash
cd scio
sbt test
```

Some examples depend on Google Cloud Platform and are excluded by default if GCP credentials are missing. To enable them, authenticate yourself for GCP, set up default credentials and restart sbt.

```bash
gcloud auth application-default login
sbt test
```

Alternatively you can populate pre-generated cache for BigQuery schemas to bypass GCP access.
Define `bigquery.project` as a system property. The value can by anything since we'll hit cache instead.

```bash
./scripts/gen_schemas.sh
sbt -Dbigquery.project=dummy-project test
```

Tasks within the 'it' (integration testing) configuration `IntegrationTest/{compile,test}` currently require access to datasets hosted in an internal Spotify project.
External users must authenticate against their own GCP project, through the steps outlined in @ref[Getting Started](../Getting-Started.md).

## IntelliJ IDEA

When opening the project in IntelliJ IDEA, tick "Use sbt shell:" both "for imports" and "for builds".
