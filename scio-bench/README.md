scio-bench
==========

# Usage

To run all benchmarks:

```bash
sbt -Dscio.version=<SCIO_VERSION> -Dbeam.version=<BEAM_VERSION> run
```

To run benchmarks whose names match a regular expression:

```bash
sbt -Dscio.version=<SCIO_VERSION> -Dbeam.version=<BEAM_VERSION> run --name=<REGEX>
```

To run benchmarks on current Scio repositority:
```bash
sbt "scio-test/it:run ..."
```
