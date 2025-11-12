import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import com.typesafe.sbt.site.SitePlugin
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.util.Try
import scala.io.Source

object SbtSoccoPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = noTrigger
  override def requires = JvmPlugin && SitePlugin

  val autoImport: SbtSoccoKeys.type = SbtSoccoKeys
  import autoImport._
  import SitePlugin.autoImport._

  override lazy val projectSettings = soccoSettings(ThisProject)

  override lazy val globalSettings =
    Def.settings(commands += enableSoccoCommand)

  def soccoSettings(p: ProjectReference, onCompile: Boolean = false) =
    Def.settings(
      soccoOnCompile.in(p) := onCompile,
      soccoPackage := Nil,
      soccoOut := target.value / "socco",
      soccoIndex := index.value,
      scalacOptions.in(p) ++= Def.taskDyn {
        if (soccoOnCompile.in(p).value) {
          Def.task(
            Seq(
              soccoOut.in(p).?.map(_.map(s"-P:socco:out:" + _)).value,
              soccoHeader.in(p).?.map(_.map(s"-P:socco:header:" + _)).value,
              soccoFooter.in(p).?.map(_.map(s"-P:socco:footer:" + _)).value,
              soccoStyle.in(p).?.map(_.map(s"-P:socco:style:" + _)).value
            ).flatten ++ soccoPackage
              .in(p)
              .map(_.map(s"-P:socco:package_" + _))
              .value
          )
        } else {
          Def.task(Nil: Seq[String])
        }
      }.value,
      autoCompilerPlugins.in(p) := true,
      (Compile / compile).in(p) := Def.taskDyn {
        val default = (Compile / compile).in(p).taskValue
        if (soccoOnCompile.in(p).value) {
          // Generate scio-examples/target/site/index.html
          Def.task {
            soccoIndex.in(p).?.value
            default.value
          }
        } else {
          Def.task(default.value)
        }
      }.value,
      makeSite / mappings ++= Option(soccoOut.value.listFiles)
        .fold(Seq.empty[(File, String)]) {
          _.map(f => (f, f.getName)).toSeq
        }
    )

  private[this] def projectsWithSocco(
                                       state: State
                                     ): Seq[(ProjectRef, Boolean)] = {
    val extracted = Project.extract(state)
    for {
      p <- extracted.structure.allProjectRefs
      onCompile <- soccoOnCompile.in(p).get(extracted.structure.data)
    } yield p -> onCompile
  }

  private[this] lazy val enableSoccoCommand = Command.command("enableSocco") {
    s =>
      val extracted = Project.extract(s)
      val settings: Seq[Setting[_]] = for {
        (p, onCompile) <- projectsWithSocco(s)
        if !onCompile
        setting <- soccoSettings(p, onCompile = true)
      } yield setting

      extracted.appendWithoutSession(settings, s)
  }

  private[this] case class ExampleSource(
                                          file: String,
                                          section: String,
                                          title: String,
                                          url: String
                                        )

  private[this] val read: File => Option[List[String]] =
    file => Try(Source.fromFile(file).getLines.toList).toOption

  private[this] def index: Def.Initialize[Task[File]] =
    Def.task {
      val header =
        soccoHeader.?.map(_.map(read.andThen(_.mkString("\n")))).value
          .getOrElse {
            s"<title>${name.value}</title>"
          }
      val footer =
        soccoFooter.?.map(_.map(read.andThen(_.mkString("\n")))).value
          .getOrElse("")
      val items = sources.value
        .groupBy(_.section)
        .map { case (section, scs) =>
          s"""### $section
             |${scs
            .map { s =>
              s"- [${s.file}](${s.file}.html) ([source](${s.url})) - ${s.title}"
            }
            .mkString("\n")}""".stripMargin
        }
        .mkString("\n")

      val html =
        s"""<!DOCTYPE html>
           |<html>
           |<head>
           |<link media="all" rel="stylesheet"
           |      href="https://bootswatch.com/4/spacelab/bootstrap.css" />
           |$header
           |</head>
           |<body>
           |<textarea hidden id="sourceTA">
           |$items
           |</textarea>
           |<div id="targetDiv" />
           |$footer
           |<script src="https://unpkg.com/showdown/dist/showdown.min.js"></script>
           |<script>
           |  const text = document.getElementById('sourceTA').value;
           |  const target = document.getElementById('targetDiv');
           |  const converter = new showdown.Converter();
           |  target.innerHTML = converter.makeHtml(text);
           |</script>
           |</html>""".stripMargin

      soccoOut.value.mkdirs()
      val file = soccoOut.value / "index.html"
      val out = new PrintWriter(file)
      out.println(html)
      out.close()
      file
    }

  private[this] def sources: Def.Initialize[Task[List[ExampleSource]]] =
    Def.task {
      val examplePattern = "^\\s*// Example:\\s*(.+)".r
      val files = Files
        .walk(Paths.get((Compile / scalaSource).value.toURI()))
        .collect(Collectors.toList())
        .asScala
        .iterator
        .filter(Files.isRegularFile(_))
        .map(_.toFile())
        .toList

      files
        .flatMap { f =>
          val lines = read(f).getOrElse(Nil)
          lines.filter(_.startsWith("// Example:")) match {
            case Nil       => None
            case head :: _ =>
              // val section = s"${f.getParentFile.getName}/"
              val section =
                (Compile / scalaSource).value
                  .relativize(f)
                  .fold("") { p =>
                    Option(p.getParent).fold("")(_.replace("/", "."))
                  }
              val title = examplePattern.unapplySeq(head).get.head
              val url = scmInfo.value
                .map { info =>
                  val path = (ThisBuild / baseDirectory).value
                    .relativize(f)
                    .fold("")(_.toString)
                  s"${info.browseUrl}/blob/master/$path"
                }
                .getOrElse("")

              Some(ExampleSource(f.getName, section, title, url))
          }
        }
        .sortBy(_.file)
    }
}

object SbtSoccoKeys {
  val soccoOut = settingKey[File]("to specify the output directory")
  val soccoStyle = settingKey[File]("path to specify a custom stylesheet")
  val soccoHeader = settingKey[File]("path to specify a custom HTML header")
  val soccoFooter = settingKey[File]("path to specify a custom HTML footer")
  val soccoPackage =
    settingKey[List[String]](
      "$packageName:$scalaDocUrl to specify a Scala API doc to link"
    )
  val soccoIndex = taskKey[File]("Generates examples/index.html")
  val soccoOnCompile: SettingKey[Boolean] = settingKey[Boolean]("Socco doc's")
}