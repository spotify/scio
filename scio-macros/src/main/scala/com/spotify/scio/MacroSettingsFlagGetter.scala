package com.spotify.scio

trait MacroSettingsFlagGetter {
  protected def getFlag(settings: List[String])(name: String, default: FeatureFlag): FeatureFlag = {
    val ss: Map[String, String] =
      settings
        .map(_.split("="))
        .flatMap {
          case Array(k, v) => Some(k.trim -> v.trim)
          case _           => None
        }
        .toMap

    ss.get(name)
      .map {
        case "true" =>
          FeatureFlag.Enable
        case "false" =>
          FeatureFlag.Disable
        case v =>
          throw new IllegalArgumentException(
            s"""Invalid value for setting -Xmacro-settings:$name,""" +
              s"""expected "true" or "false", got $v"""
          )
      }
      .getOrElse(default)
  }
}