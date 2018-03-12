package com.spotify.scio.bigquery.validation


object ValidationProviderFinder {

  def getProvider: ValidationProvider = {
    try {
      // Load the class dynamically at compile time and runtime
      Class.forName(System.getenv("VALIDATION_PROVIDER"))
        .newInstance()
        .asInstanceOf[ValidationProvider]
    } catch {
      case _: Throwable => {
        println("Using DummyValidationProvider")
        new DummyValidationProvider
      }
    }
  }
}
