package com.spotify.scio.util

import java.io.File
import org.ini4j.Ini
import org.slf4j.LoggerFactory

object GCloudConfigUtils {
  val logger = LoggerFactory.getLogger(GCloudConfigUtils.getClass)
  val PROJECT_ENV_NAME = "GCLOUD_PROJECT"
  val CLOUD_CONFIG_ENV_NAME = "CLOUDSDK_CONFIG"

  /**
   * Check:
   * 1. system property for project name
   * 2. environmental variable for project name
   * 3. configuration files for project name
   */
  def getGCloudProjectId: Option[String] = {
    val projectId = sys.props.getOrElse(PROJECT_ENV_NAME,
                                        sys.env.getOrElse(PROJECT_ENV_NAME, null))

    try {
      if (projectId != null) Some(projectId) else getProjectIdFromConfigFile
    } catch {
      case e: Exception =>
        logger.debug(s"Could not find Google Cloud project id, due to ${e.getMessage}", e)
        None
    }
  }

  private def getConfigDir: File = {
    if (sys.env.contains(CLOUD_CONFIG_ENV_NAME)) {
      new File(sys.env(CLOUD_CONFIG_ENV_NAME))
    } else {
      new File(sys.props("user.home"), ".config/gcloud")
    }
  }

  /**
   * Search for GCloud config file (and return first), in the order of:
   * 1. [gcloud-config-dir]/configurations/config_scio
   * 2. [gcloud-config-dir]/configurations/config_default
   * 3. [gcloud-config-dir]/properties
   *
   * where gcloud-config-dir can be for example: `~/.config/gcloud`
   */
  private def getConfigFile(configDir: File): File = {
    val possibleConfigs = List("configurations/config_scio",
                               "configurations/config_default",
                               "properties")

    val configFile = possibleConfigs.find(p => new File(configDir, p).canRead)

    configFile match {
      case Some(path) => new File(configDir, configFile.get)
      case None => logger.debug("Could not find Google Cloud config files"); null
    }
  }

  private def getProjectIdFromConfigFile: Option[String] = {
    val configDir = getConfigDir
    val configFile = getConfigFile(configDir)

    if (configFile != null) {
      val ini = new Ini(configFile)
      logger.debug(s"Looking for Google Cloud project name in ${configFile.getCanonicalPath}")
      Option(ini.get("core").get("project"))
    } else {
      None
    }
  }
}
