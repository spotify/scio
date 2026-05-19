import scala.tools.nsc
import nsc.{Global, Phase}
import nsc.plugins.{Plugin, PluginComponent}

class Socco(val global: Global) extends Plugin {
  import global._

  val name = "socco"
  val description = """
    A plugin to generate HTML documents that display your comments alongside your code.
  """
  val components = List[PluginComponent](Component)

  // Plugin options
  var userStyles = Option.empty[String]
  var out = new java.io.File(sys.env.get("SOCCO_OUT").getOrElse("."))
  var packages = Map.empty[String, String]
  var header = Option.empty[String]
  var footer = Option.empty[String]

  // Easier scala regex
  implicit class RegexContext(sc: StringContext) {
    def regex =
      new scala.util.matching.Regex(
        sc.parts.mkString,
        sc.parts.tail.map(_ => "_"): _*
      )
  }

  override def init(
    options: List[String],
    callback: (String) => Unit
  ): Boolean = {
    import scala.util.Try
    import scala.io.Source

    def error(t: Throwable) = callback(t.getMessage)

    Try {
      options.map {
        case regex"out:(.*)$directory" =>
          out = new java.io.File(directory)
        case regex"style:(.*)$file" =>
          val lines = Source.fromFile(file).getLines().mkString("\n")
          userStyles = Some(lines)
        case regex"header:(.*)$file" =>
          header = Some(Source.fromFile(file).getLines().mkString("\n"))
        case regex"footer:(.*)$file" =>
          footer = Some(Source.fromFile(file).getLines().mkString("\n"))
        case regex"package_([^:]*)$packageName:(.*)$url" =>
          packages = packages + (packageName -> url)
        case oops =>
          new Exception(s"Option not recognized, $oops")
      }
    }
      .fold(t => { error(t); false }, _ => true)
  }

  // A type annotation with a link to the scalaDoc
  case class TypeAnnotation(link: String, display: String)

  // Token styles
  sealed trait Style
  case object Keyword extends Style
  case object StringLiteral extends Style
  case object NumberLiteral extends Style
  case object Default extends Style
  case class Identifier(maybeType: Option[TypeAnnotation]) extends Style

  // A code token
  case class Token(start: Int, text: String, style: Style) {
    lazy val length = text.length
    lazy val end = start + length
    override def hashCode = start
    override def equals(other: Any) =
      other match {
        case that: Token => this.start == that.start
        case _           => false
      }
  }

  // A block of source (either code or comment)
  case class Block(start: Int, text: String) {
    lazy val length = text.length
    lazy val end = start + length
    def isEmpty = text.isEmpty
  }

  private object Component extends PluginComponent {
    val global: Socco.this.global.type = Socco.this.global
    val runsAfter = List("typer")
    val phaseName = Socco.this.name
    override val description = Socco.this.description
    def newPhase(prev: Phase) = new SoccoPhase(prev)

    class SoccoPhase(prev: Phase) extends StdPhase(prev) {
      override def name = Socco.this.name

      def apply(unit: CompilationUnit): Unit = {
        // First, we check if we need to process this unit,
        // and we split the source into (comments,code) blocks.
        val (headerOffset, maybeTitle, blocks) = splitInBlocks(unit)

        maybeTitle.foreach { case title =>
          // Now we tokenize the source code using
          // the scala compiler analyzer.
          val tokens = tokenize(unit, headerOffset)

          // Try to type the relevant tokens and to generate
          // links to the scala documentation.
          val typedTokens = typeTokens(unit, tokens, headerOffset)

          // Finally generate the HTML output
          generate(unit, typedTokens, title, blocks)
        }
      }

      def splitInBlocks(
        unit: CompilationUnit
      ): (Int, Option[String], Seq[(Block, Block)]) = {
        val src = new String(unit.source.content)
        val headerOffset = src.indexOf("\n// Example:") + 1
        val lines = src.substring(headerOffset).split("\n")
        val maybeTitle = lines.headOption.flatMap {
          case regex"// Example:(.*)$title" => Some(title.trim)
          case _                            => None
        }
        def isComment(line: String): Option[String] =
          """^\s*//\s*(.*)$""".r.unapplySeq(line).flatMap(_.lift(0))
        val (_, blocks) = maybeTitle
          .fold(lines)(_ => lines.tail)
          .foldLeft(
            (maybeTitle.fold(0)(_ => lines.head.length + 1)) -> List
              .empty[Either[Block, Block]]
          ) { case ((offset, blocks), line) =>
            (offset + line.length + 1) -> (blocks match {
              case Nil =>
                isComment(line).fold[List[Either[Block, Block]]] {
                  Right(Block(offset, line)) :: Left(Block(0, "")) :: Nil
                }(comment => Left(Block(offset, comment)) :: Nil)
              case block :: rest =>
                isComment(line).fold[List[Either[Block, Block]]] {
                  block match {
                    case Left(_) =>
                      Right(Block(offset, line)) :: block :: rest
                    case Right(block) =>
                      Right(
                        block.copy(text = s"${block.text}\n${line}")
                      ) :: rest
                  }
                } { comment =>
                  block match {
                    case Left(block) =>
                      Left(
                        block.copy(text = s"${block.text}\n${comment}")
                      ) :: rest
                    case Right(_) =>
                      Left(Block(offset, comment)) :: block :: rest
                  }
                }
            })
          }

        (
          headerOffset,
          maybeTitle,
          blocks.reverse
            .collect { case Left(b) => b }
            .zip(
              blocks.reverse.collect { case Right(b) => b }
            )
            .dropWhile { case (a, b) => a.isEmpty && b.isEmpty }
        )
      }

      def tokenize(unit: CompilationUnit, headerOffset: Int): Seq[Token] = {
        val source = unit.source
        val tokens = collection.mutable.HashMap.empty[Int, (Int, Int)]

        class Tokenizer extends syntaxAnalyzer.UnitScanner(unit) {
          override def error(off: Int, msg: String) = ()
          override def incompleteInputError(off: Int, msg: String) = ()
          override def nextToken() = {
            val offset0 = offset
            val code = token
            super.nextToken()
            val length = (lastOffset - offset0) max 1
            tokens += ((offset0, (length, code)))
          }
        }

        if (unit.isJava) sys.error("😂")
        else {
          new syntaxAnalyzer.UnitParser(unit) {
            override def newScanner = new Tokenizer
          }.parse()
        }

        tokens.toSeq
          .sortBy(_._1)
          .map { case (start, (length, code)) =>
            import nsc.ast.parser.Tokens._

            val text = source.content.slice(start, start + length)
            Token(
              math.max(0, start - headerOffset),
              new String(text),
              code match {
                case STRINGLIT | STRINGPART | INTERPOLATIONID => StringLiteral
                case x if isLiteral(x)                        => NumberLiteral
                case x if isIdentifier(x)                     => Identifier(None)
                case ABSTRACT | CASE | CATCH | CLASS | DEF | DO | ELSE | EXTENDS | FINAL | FINALLY |
                    FOR | IF | IMPLICIT | IMPORT | LAZY | NEW | MACRO | MATCH | OBJECT | PACKAGE |
                    PRIVATE | PROTECTED | RETURN | SUPER | TRY | VAL | VAR | WHILE | YIELD |
                    CASECLASS =>
                  Keyword
                case _ => Default
              }
            )
          }
          .filterNot(_.length <= 0)
      }

      def typeTokens(
        unit: CompilationUnit,
        tokens: Seq[Token],
        headerOffset: Int
      ): Seq[Token] = {
        import collection.mutable._
        val tokenSymbols = HashMap(
          tokens.map(_ -> ListBuffer.empty[Symbol]): _*
        )
        (new Traverser {
          override def traverse(tree: Tree) = {
            if (tree.pos != NoPosition) {
              tokens.find(_.start == tree.pos.point - headerOffset).foreach { token =>
                tree match {
                  case tree: TypeTree =>
                    tokenSymbols(token) += tree.symbol
                  case tree if tree.hasSymbolField =>
                    tokenSymbols(token) += tree.symbol
                  case _ =>
                }
              }
            }
            super.traverse(tree)
          }
        }).traverse(unit.body)

        // ==> Reverse engineering of the scalaDoc URL patterns
        def scalaDocLink(symbol: Symbol): Option[String] = {
          def go(symbol: Symbol): Option[(String, String, String)] =
            symbol match {
              case NoSymbol =>
                None
              case s if s.isRoot =>
                None
              case p: ModuleSymbol if p.hasPackageFlag =>
                Some(
                  (
                    go(p.owner).fold("") { case (p, s, _) => p + s } + p.name,
                    "/",
                    "/index.html"
                  )
                )
              case p: PackageClassSymbol =>
                Some(
                  (
                    go(p.owner).fold("") { case (p, s, _) => p + s } + p.name,
                    "/",
                    "/index.html"
                  )
                )
              case o: ModuleSymbol =>
                Some(
                  (
                    go(o.owner).fold("") { case (p, s, _) =>
                      p + s
                    } + o.name + "$",
                    "$",
                    ".html"
                  )
                )
              case o: ModuleClassSymbol =>
                Some(
                  (
                    go(o.owner).fold("") { case (p, s, _) =>
                      p + s
                    } + o.name + "$",
                    "$",
                    ".html"
                  )
                )
              case c: ClassSymbol =>
                Some(
                  (
                    go(c.owner).fold("") { case (p, s, _) => p + s } + c.name,
                    "$",
                    ".html"
                  )
                )
              case m: MethodSymbol =>
                val methodAnchor = {
                  val returnType =
                    m.returnType.toString.replaceAll("""\s""", "")
                  m.name.toString + m.info.toString
                    .replaceAll("""\s""", "")
                    .replaceAll(
                      s"(=>)?\\Q$returnType\\E${'$'}",
                      s":$returnType"
                    )
                }
                val url = go(m.owner).fold("") { case (p, _, s) => p + s }
                Some(
                  (
                    url.replaceAll(
                      "/package\\$\\.html$",
                      "/index.html"
                    ) + "#" + methodAnchor,
                    "",
                    ""
                  )
                )
              case _ =>
                None
            }
          go(symbol).map { case (name, _, suffix) => name + suffix }
        }

        def maybeTypeSymbol(symbol: Symbol): Option[TypeAnnotation] = {
          def findPackage(symbolName: String) =
            packages.find { case (name, _) =>
              (name == symbolName) || (symbolName.startsWith(name + "."))
            }

          symbol match {
            case s: Symbol if s.isImplementationArtifact => None
            case p: ModuleSymbol if p.hasPackageFlag     =>
              findPackage(p.fullName).flatMap { case (_, link) =>
                scalaDocLink(p).map { encodedPackageName =>
                  TypeAnnotation(
                    s"$link/$encodedPackageName",
                    p.fullName
                  )
                }
              }
            case o: ModuleSymbol =>
              findPackage(o.fullName).flatMap { case (_, link) =>
                scalaDocLink(o).map { encodedObjectName =>
                  TypeAnnotation(
                    s"$link/$encodedObjectName",
                    o.fullName
                  )
                }
              }
            case o: ModuleClassSymbol =>
              findPackage(o.fullName).flatMap { case (_, link) =>
                scalaDocLink(o).map { encodedObjectName =>
                  TypeAnnotation(
                    s"$link/$encodedObjectName",
                    o.fullName
                  )
                }
              }
            case m: MethodSymbol =>
              findPackage(m.owner.fullName).flatMap { case (_, link) =>
                scalaDocLink(m).map { encodedMethodName =>
                  val returnType =
                    m.returnType.toString.replaceAll("""\s""", "")
                  TypeAnnotation(
                    s"$link/$encodedMethodName",
                    s"${m.fullName}${m.info.toString
                        .replaceAll(s"(=>\\s*)?\\Q$returnType\\E${'$'}", s": $returnType")}"
                  )
                }
              }
            case t: TermSymbol =>
              findPackage(t.typeSignature.typeSymbol.fullName).flatMap { case (_, link) =>
                scalaDocLink(t.typeSignature.typeSymbol).map { encodedTypeName =>
                  TypeAnnotation(
                    s"$link/$encodedTypeName",
                    t.info.toString
                  )
                }
              }
            case c: ClassSymbol =>
              findPackage(c.fullName).flatMap { case (_, link) =>
                scalaDocLink(c).map { encodedClassName =>
                  TypeAnnotation(
                    s"$link/$encodedClassName",
                    c.fullName
                  )
                }
              }
            case _ =>
              None
          }
        }

        tokenSymbols
          .map {
            case (token @ Token(_, _, Identifier(None)), symbols) =>
              val bestTypeAnnotation =
                // The problem here is to find the best tree for the token position.
                // So we sort the trees so the best symbol is the first of the list for
                // this token position.
                //
                // How do we now the best one? This is a difficult question but it looks logical
                // that the symbol with the name matching exactly the source code text is the best.
                // One execption though, apply(...) method application does not appear in the source
                // code and it still gives the best information, so we prefer apply when possible.
                symbols
                  .find(_.name.toString == "apply")
                  .orElse {
                    symbols.find(_.name.toString == token.text)
                  }
                  .orElse {
                    symbols.headOption
                  }
                  .flatMap(maybeTypeSymbol)
              token.copy(style = Identifier(bestTypeAnnotation))

            case (token, _) =>
              token

          }
          .toSeq
          .sortBy(_.start)
      }

      def generate(
        unit: CompilationUnit,
        tokens: Seq[Token],
        title: String,
        blocks: Seq[(Block, Block)]
      ): Unit = {
        // Format a code block as HTML, by colorizing tokens and linking identifiers
        // to the scalaDoc.
        def formatSourceCode(block: Block, tokens: Seq[Token]): String = {
          def remapTokens(tokens: Seq[Token], from: Int, to: Int) = {
            tokens
              .filterNot(t => t.end <= from)
              .filterNot(t => t.start >= to)
              .map(t => t.copy(start = t.start - from))
          }
          def escape(token: Token) = token.text.replaceAll("<", "&lt;")
          def style(token: Token) =
            token.style match {
              case Keyword =>
                s"""<span class="keyword">${escape(token)}</span>"""
              case StringLiteral =>
                s"""<span class="string">${escape(token)}</span>"""
              case NumberLiteral =>
                s"""<span class="number">${escape(token)}</span>"""
              case Identifier(None) =>
                s"""<span class="identifier">${escape(token)}</span>"""
              case Identifier(Some(t)) =>
                s"""<span class="identifier"><a target="doc" href="${t.link}" data-tooltip="<span>${t.display}</span>">${escape(
                    token
                  )}</a></span>"""
              case _ => escape(token)
            }

          val (lastPosition, html) =
            remapTokens(tokens, block.start, block.end).foldLeft(0 -> "") {
              case ((i, text), token) =>
                token.end -> (
                  if (token.start > i) {
                    text + block.text.substring(i, token.start) + style(token)
                  } else text + style(token)
                )
            }

          html + block.text.substring(lastPosition) + "&nbsp;"
        }

        // Format a comment block by styling it using markdown.
        def formatComments(
          block: Block,
          packages: Map[String, String]
        ): String = {
          import laika.api._
          import laika.format.{Markdown, HTML}
          import laika.ast._

          def findPackage(url: String) =
            packages
              .find { case (name, _) =>
                url.startsWith(name.replaceAll("[.]", "/"))
              }
              .map(_._2)

          val transformer = Transformer
            .from(Markdown)
            .to(HTML)
            .rendering {
              case (fmt, SpanLink(content, url, _, opt)) if findPackage(url.render()).isDefined =>
                fmt.element(
                  "a",
                  opt,
                  content,
                  "target" -> "doc",
                  "href" -> s"${findPackage(url.render()).get}/${url.render()}"
                )
            }
            .build

          transformer.transform(block.text) match {
            case Right(html) => html
            case Left(error) =>
              println(error.message)
              block.text
          }
        }

        val html = s"""
          ${header.getOrElse("")}
          <style>${fromClasspath("/tooltips.css")}</style>
          <style>${fromClasspath("/base.css")}</style>
          ${userStyles.map(css => s"<style>$css</style>").getOrElse("")}
          <section id="header">
            <div class="comment"><h1>${title}</h1></div>
            <div class="code"></div>
          </section>
          ${blocks.map { case (comment, code) =>
            s"""
              <section class="${if (comment.text.trim.matches("@\\w+"))
                comment.text.trim.drop(1)
              else ""}">
                <div class="comment ${if (comment.text.trim.isEmpty) "empty"
              else ""}">${formatComments(comment, packages)}</p></div>
                <div class="code ${if (code.text.trim.isEmpty) "empty"
              else ""}">${formatSourceCode(code, tokens)}</div>
              </section>
            """
          }.mkString}
          <section id="footer">
            <div class="comment"></div>
            <div class="code"></div>
          </section>
          ${footer.getOrElse("")}
          <script>${fromClasspath("/tooltips.min.js")}</script>
          <script>
            var tips = new Tooltips(document, {
              tooltip: {
                auto: true,
                place: 'top',
                effectClass: '_'
              },
              key: 'tooltip',
              showOn: 'mouseenter',
              hideOn: 'mouseleave'
            });
          </script>
        """

        import java.io._

        val sourceFile = unit.source.file.file
        val htmlFile = new File(out, s"${sourceFile.getName}.html")
        htmlFile.getParentFile.mkdirs()
        val outStream = new FileOutputStream(htmlFile)
        outStream.write(html.getBytes("utf-8"))
        outStream.close()

        println(
          s"[socco] transformed ${sourceFile} to ${htmlFile.getAbsolutePath}"
        )
      }

    }
  }

  def fromClasspath(resource: String): String = {
    Option(getClass.getResource(resource))
      .map { res =>
        val connection = res.openConnection()
        connection.setUseCaches(false)
        scala.io.Source.fromInputStream(connection.getInputStream).mkString
      }
      .getOrElse("")
  }
}
