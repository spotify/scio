object JavaOptions {
  class JavaOption(
    val option: String,
    val args: List[String],
    val isSupported: Int => Boolean
  ) {
    override def hashCode(): Int =
      41 * option.hashCode

    override def equals(other: Any): Boolean =
      other match {
        case that: JavaOption =>
          this.option == that.option &&
          this.args.size == that.args.size &&
          this.args.sorted.zip(that.args.sorted).forall { case (s1, s2) => s1 == s2 }
        case _ => false
      }

    override def toString =
      (option :: args).mkString("JavaOption(", " ", ")")
  }

  def property(
    key: String,
    value: String,
    isSupported: Int => Boolean = _ => true
  ): JavaOption =
    new JavaOption(s"-D$key=$value", List.empty, isSupported)
  def extra(
    name: String,
    arguments: List[String] = List.empty,
    isSupported: Int => Boolean = _ => true
  ): JavaOption = new JavaOption(s"-X$name", arguments, isSupported)

  def minHeapSize(size: String): JavaOption =
    extra(s"ms$size")

  def maxHeapSize(size: String): JavaOption =
    extra(s"mx$size")

  def advanced(
    name: String,
    arguments: List[String] = List.empty,
    isSupported: Int => Boolean = _ => true
  ): JavaOption = new JavaOption(s"-XX:$name", arguments, isSupported)

  def addOpens(module: String, `package`: String, reflectingModule: String) =
    new JavaOption("--add-opens", List(s"$module/${`package`}=$reflectingModule"), _ >= 17)
  def optionsForVersion(
    javaVersion: Int,
    proposedJavacOptions: Set[JavaOption]
  ): Set[JavaOption] =
    proposedJavacOptions.filter(_.isSupported(javaVersion))

  def tokensForVersion(
    javaVersion: Int,
    proposedJavacOptions: Set[JavaOption]
  ): Seq[String] =
    optionsForVersion(javaVersion, proposedJavacOptions).toList
      .flatMap(opt => opt.option :: opt.args)

  def defaults(javaVersion: Int): Seq[String] =
    tokensForVersion(
      javaVersion,
      Set(
        property("file.encoding", "UTF-8"),
        addOpens("java.base", "java.util", "ALL-UNNAMED"),
        addOpens("java.base", "java.lang.invoke", "ALL-UNNAMED"),
        addOpens("java.base", "java.lang", "ALL-UNNAMED"),
        addOpens("java.base", "java.nio", "ALL-UNNAMED")
      )
    )

  private lazy val runJvmDefaults = Set(
    minHeapSize("512m"),
    maxHeapSize("2G"),
    advanced("+UseParallelGC")
  )

  private lazy val runScioDefaults = Set(
    property("scio.ignoreVersionWarning", "true"),
    property("org.slf4j.simpleLogger.defaultLogLevel", "info")
  ) ++ Set(
    "bigquery.project",
    "bigquery.secret",
    "cloudsql.sqlserver.password"
  ).flatMap(prop => sys.props.get(prop).map(value => property(prop, value)))

  def testDefaults(javaVersion: Int): Seq[String] =
    tokensForVersion(
      javaVersion,
      runJvmDefaults ++ runScioDefaults ++ Set(
        property("org.slf4j.simpleLogger.logFile", "scio.log")
      )
    )

  def runDefaults(javaVersion: Int): Seq[String] =
    tokensForVersion(
      javaVersion,
      runJvmDefaults ++ runScioDefaults
    )
}
