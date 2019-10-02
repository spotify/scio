# Build

## Getting the source

```bash
git clone https://github.com/spotify/scio.git
cd scio
```

## Compiling

Copy the pre generated BigQuery schemas:
Bootstrap BigQuery schemas cache and flags if you don't have Google Cloud Platform credentials.

```bash
./scripts/bootstrap.sh
```

Build the source

```bash
sbt compile
```
