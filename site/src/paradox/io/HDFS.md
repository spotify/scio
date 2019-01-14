# HDFS

## Settings for Scio version >= `0.4.0`

Hadoop configuration is loaded from 'core-site.xml' and 'hdfs-site.xml' files based upon the `HADOOP_CONF_DIR` and `YARN_CONF_DIR` environment variables. Set one of them to the location of your Hadoop configuration directory.

## Settings for Scio version < `0.4.0`

To access HDFS from a Scio job, Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`, etc.) must be available in classpath of both runner's client and workers. There is multiple ways to achieve it:

 * place your configuration files in java startup classpath via `java -cp <classpath>`
 * place your configuration files in `src/main/resources`

The following example handles local, GCS and HDFS in the same job code.

```scala
import com.spotify.scio.hdfs._

val input = args("input")
val output = args("output")

val pipe = if (input.startsWith("hdfs://")) {
  sc.hdfsTextFile(input)  // HDFS
} else {
  sc.textFile(input)  // local or GCS
}

val result = pipe
  .map(/*...*/)
  .reduce(/*...*/)

if (output.startsWith("hdfs://")) {
  result.saveAsHdfsTextFile(output)  // HDFS
} else {
  result.saveAsTextFile(output)  // local or GCS
}
```

### Running locally
You local environment needs access to the name and data nodes in the Hadoop cluster when running a job locally.

### Running on Dataflow service
When running a job on the Dataflow managed service, worker VM instances need access to the Hadoop cluster. Set `--network`/`--subnetwork` to one that has proper setup, e.g. VPN to your on-premise cluster or another cloud provider.

### Use HDFS from Scio REPL

Assuming that you have your configuration files in `hadoop-conf.jar`, for example:
```
$ jar -tf hadoop-conf.jar
META-INF/
META-INF/MANIFEST.MF
core-site.xml
hdfs-site.xml
```
Assembly HDFS submodule:
```
$ sbt 'project scio-hdfs' assembly
```
Start Scio REPL:
```
$ java -cp scio-repl/target/scala-2.11/scio-repl-0.3.4-SNAPSHOT.jar:scio-hdfs/target/scala-2.11/scio-hdfs-assembly-0.3.4-SNAPSHOT.jar:hadoop-conf.jar com.spotify.scio.repl.ScioShell
```

## Common issues

#### Permission denied: user=root, access=WRITE, inode=...

```org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException): Permission denied: user=root, access=WRITE, inode=... ```

By default your Dataflow job is run as `root` user on worker containers. If you are using Simple Authentication for HDFS, Scio provides a parameter in HDFS sink methods to specify remote user via `username` parameter.

#### java.io.IOException: No FileSystem for scheme: hdfs

This is a common issue with fat jars built with [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin. It's related to Hadoop filesystem registration services file - there are multiple jars that provide the same file with configuration, in your case most probably only one is picked (and it's not the one holding configuration for HDFS filesystem).

###### Possible solutions

* 1st solution - if you are using [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin - merge those service files, so in your sbt merge configuration add strategy to filter distinct lines, like this:

```
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
    case s => old(s)
  }
}
```

 * 2nd solution - give [sbt-pack](https://github.com/xerial/sbt-pack) a try, it creates a directory/tarball of all dependency jars without explicit merging/fatjars.

More on this/related issues [here](https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file).
