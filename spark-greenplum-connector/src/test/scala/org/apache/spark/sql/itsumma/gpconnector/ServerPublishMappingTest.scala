package org.apache.spark.sql.itsumma.gpconnector

object ServerPublishMappingTest {
  def main(args: Array[String]): Unit = {
    val mappings = ServerPublishMapping.parse(
      "172.26.32.28=10.150.0.5:20101-20110->20111-20120," +
        "172.26.32.113=10.150.0.5:20111-20120->20111-20120," +
        "172.26.32.114=10.150.0.5:20121-20130->20111-20120"
    )

    assert(mappings("172.26.32.28").publishPortFor(20111) == 20101)
    assert(mappings("172.26.32.28").publishPortFor(20114) == 20104)
    assert(mappings("172.26.32.28").publishPortFor(20120) == 20110)
    assert(mappings("172.26.32.113").publishPortFor(20119) == 20119)
    assert(mappings("172.26.32.114").publishPortFor(20115) == 20125)

    val defaultBinding = PublishBinding("172.26.32.28", None)
    assert(defaultBinding.publishHost == "172.26.32.28")
    assert(defaultBinding.publishPortFor(43000) == 43000)
    assert(defaultBinding.localPortRange.isEmpty)
    assert(ServerPublishMapping.parse("").isEmpty)

    expectFailure("old mapping syntax") {
      ServerPublishMapping.parse("172.26.32.28=10.150.0.5:20101-20110")
    }
    expectFailure("different range sizes") {
      ServerPublishMapping.parse("172.26.32.28=10.150.0.5:20101-20105->20111-20120")
    }
    expectFailure("duplicate executor address") {
      ServerPublishMapping.parse(
        "172.26.32.28=10.150.0.5:20101-20110->20111-20120," +
          "172.26.32.28=10.150.0.5:20121-20130->20111-20120"
      )
    }
    expectFailure("local port outside range") {
      mappings("172.26.32.28").publishPortFor(20200)
    }

    println("SERVER_PUBLISH_MAPPING_TEST_OK")
  }

  private def expectFailure(name: String)(body: => Unit): Unit = {
    try {
      body
      throw new AssertionError(s"Expected failure: $name")
    } catch {
      case _: IllegalArgumentException =>
    }
  }
}
