package com.spotify.scio.cosmosdb

import scribe.filter.{level, packageName, select}
import scribe.format.{
  bold,
  closeBracket,
  cyan,
  fileName,
  green,
  groupBySecond,
  italic,
  levelColoredPaddedRight,
  line,
  mdcMultiLine,
  messages,
  multiLine,
  newLine,
  openBracket,
  position,
  space,
  string,
  threadName,
  time,
  Formatter
}
import scribe.{Level, Logger}

object Utils {

  def initLog(): Unit = {

    val formatter1 = Formatter.fromBlocks(
      groupBySecond(
        newLine,
        openBracket,
        bold(levelColoredPaddedRight),
        space,
        cyan(bold(time)),
        closeBracket,
        space,
        string("("),
        italic(threadName),
        string(")"),
        space,
        green(position),
        space,
        string("("),
        fileName,
        string(":"),
        line,
        string(")"),
        newLine
      ),
      // openBracket,
      // bold(levelColoredPaddedRight),
      multiLine(messages),
      mdcMultiLine
    )

    Logger.root
      .clearHandlers()
      .clearModifiers()
      .withHandler(
        formatter = formatter1,
        minimumLevel = Some(Level.Info),
        modifiers = List(
          select(packageName.startsWith("com.azure.cosmos")).exclude(level < Level.Warn)
        )
      )
      .replace()
  }
}
