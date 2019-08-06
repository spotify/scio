# Build

## Getting the source

```bash
git clone https://github.com/spotify/scio.git
```

## Compiling

Copy the pre generated BigQuery schemas:

```bash
cp -r scripts/bigquery .bigquery
```

Define `bigquery.project` as a system property. You can use any value since we will be using the pre generated schemas.

```bash
sbt -Dbigquery.project=dummy compile
```
