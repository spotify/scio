# OverrideTypeProvider

The @scaladoc[`OverrideTypeProvider`](com.spotify.scio.bigquery.validation.OverrideTypeProvider) trait allows the user to provide custom mappings from BigQuery types to custom Scala types.

This can be used for a number of use cases:
* Using higher level types in Scio in order to be explicit about what your data is
* Custom code can be run when you create new objects to do things like data validation or simple transformation

The methods in the Scala trait allow you to inspect the incoming types from BigQuery and decide if you'd like to provide an alternative type mapping to your own custom type.  You also must tell Scio how to convert your types back into BigQuery data types.

# Setup

Once you implement the `OverrideTypeProvider` with your own custom types you can supply it to the `OverrideTypeProviderFinder` by specifying a JVM System property as below.

```scala
System.setProperty(
  "override.type.provider",
  "com.spotify.scio.bigquery.validation.SampleOverrideTypeProvider")
```

Since this feature uses Scala macros you must do this at initialization time.  One easy way to do this is in the `build.sbt` file for your project.  This would look like below.

```scala
initialize in Test ~= { _ => System.setProperty(
  "override.type.provider",
  "com.spotify.scio.bigquery.validation.SampleOverrideTypeProvider")
}
```

Currently only one `OverrideTypeProvider` is allowed per sbt project.

This provider is loaded using Reflection at macro expansion time and at runtime as well.

If this System property isn't specified then Scio falls back to the normal default behavior.

# Implementation

Custom implementations of the `OverrideTypeProvider` should implement the methods as described below.

```scala
def shouldOverrideType(tfs: TableFieldSchema)
```
- This is the first point of entry and is called when we use macros to create case classes for `fromQuery`, `fromSchema`, and `fromTable`.

```scala
def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema)
```
- This is called when the above `shouldOverrideType` returns `true`.  Expected return value is a `c.Tree` representing the Scala type you'd like to use for this mapping.

```scala
def shouldOverrideType(c: blackbox.Context)(tpe: c.Type)
```
- This is called when we do conversions to and from a `TableRow` internally and your generated case class.

```scala
def createInstance(c: blackbox.Context)(tpe: c.Type, tree: c.Tree)
```
- This is called when the above `shouldOverrideType` returns `true`.  Expected return value is a `c.Tree` representing how to create a new instance of your custom Scala type.

```scala
def shouldOverrideType(tpe: Type)
```
- This is called at runtime when we do any operations on the schema directly.

```scala
def getBigQueryType(tpe: Type)
```
- This is called when the above `shouldOverrideType` returns `true`.  It should return the `String` representation for the BigQuery column type for your class and field now.

```scala
def initializeToTable(c: blackbox.Context)(modifiers: c.universe.Modifiers,
                                           variableName: c.universe.TermName,
                                           tpe: c.universe.Tree)
```
- This is called once per field when we extend the case classes for `toTable` examples.
