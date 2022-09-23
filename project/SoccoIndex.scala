/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

object SoccoIndex {
  import java.io.{File, PrintWriter}

  val baseUrl = "https://github.com/spotify/scio/blob/master/"

  val basePath = "scio-examples/src/%s/scala/com/spotify/scio/examples"
  val mainPath = new File(basePath.format("main"))
  val testPath = new File(basePath.format("test"))

  val header =
    """<!DOCTYPE html>
      |<html>
      |  <head>
      |    <title>Scio Examples</title>
      |    <link rel="shortcut icon" href="../images/favicon.ico">
      |    <style>
      |      body,input{font-family:"Roboto","Helvetica Neue",Helvetica,Arial,sans-serif}
      |    </style>
      |  </head>
      |  <body>
      |    <textarea hidden id="sourceTA">
    """.stripMargin
  val footer =
    """    </textarea>
      |    <div id="targetDiv" />
      |    <script src="https://unpkg.com/showdown/dist/showdown.min.js"></script>
      |    <script>
      |      const text = document.getElementById('sourceTA').value;
      |      const target = document.getElementById('targetDiv');
      |      const converter = new showdown.Converter();
      |      const html = converter.makeHtml(text);
      |      target.innerHTML = html;
      |    </script>
      |  <body>
      |</html>
    """.stripMargin

  def ls(f: File): Array[File] = {
    val fs = f.listFiles()
    fs ++ fs.filter(_.isDirectory).flatMap(ls)
  }

  def isScala(f: File): Boolean = f.isFile && f.getName.endsWith(".scala")

  case class Source(
    file: String,
    section: String,
    title: String,
    url: String,
    objects: List[String]
  )

  // Find all source files
  private def sources = {
    val examplePattern = "^\\s*// Example:\\s*(.+)".r
    val objectPattern = "^\\s*object\\s*(\\w+)\\s*\\{?".r
    ls(mainPath)
      .filter(isScala)
      .flatMap { f =>
        val lines = scala.io.Source.fromFile(f).getLines().toList
        val e = lines.filter(_.startsWith("// Example:"))
        if (e.nonEmpty) {
          val section = f.getParent.replaceFirst(mainPath.toString, "") + "/"
          val title = examplePattern.unapplySeq(e.head).get.head
          val objects = lines
            .filter(_.startsWith("object "))
            .map(l => objectPattern.unapplySeq(l).get.head)
          Some(Source(f.getName, section, title, baseUrl + f, objects))
        } else {
          None
        }
      }
  }

  // Find mapping between source jobs and tests
  private def tests = {
    val testPattern = "^\\s*JobTest\\[(.+)\\.type\\]".r
    ls(testPath)
      .filter(isScala)
      .flatMap { f =>
        val lines = scala.io.Source.fromFile(f).getLines().toList
        val e = lines.filter(_.trim.startsWith("JobTest["))
        if (e.nonEmpty) {
          e.map { l =>
            val obj = testPattern.unapplySeq(l).get.head.split("\\.").last
            obj -> (baseUrl + f)
          }
        } else {
          Nil
        }
      }
      .toMap
  }

  def mappings: Seq[(File, String)] =
    sources
      .map(s => new File(s"scio-examples/target/site/${s.file}.html") -> s"examples/${s.file}.html")

  def generate(outFile: File): File = {
    outFile.getParentFile.mkdirs()
    val out = new PrintWriter(outFile)
    out.println(header)
    var section = ""
    sources.sortBy(s => (s.section, s.file)).foreach { s =>
      if (section != s.section) {
        section = s.section
        out.println()
        out.println("### " + section)
        out.println()
      }
      val test = s.objects.flatMap(tests.get).headOption match {
        case Some(url) => s", [test]($url)"
        case None      => ""
      }
      out.println(s"- [${s.file}](${s.file}.html) ([source](${s.url})$test) - ${s.title}")
    }
    out.println(footer)
    out.close()
    outFile
  }
}
