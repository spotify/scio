package com.spotify.scio.bigquery.validation


object ValidationProviderFinder {

  def getProvider: ValidationProvider = {

  /**
    println(Class.forName("com.spotify.semantic.SemanticTypeValidationProvider"))
    val x = ServiceLoader.load(classOf[ValidationProvider])
      .iterator()

    if (x.hasNext) {
      x.next()
    } else {
      println("Using old now")
      new DummyValidationProvider
    }
  */
    try {
      Class.forName(System.getenv("VALIDATION_PROVIDER"))
        .newInstance()
        .asInstanceOf[ValidationProvider]
    } catch {
      case _: Throwable => println("Using old now")
        new DummyValidationProvider
    }
  }
}
