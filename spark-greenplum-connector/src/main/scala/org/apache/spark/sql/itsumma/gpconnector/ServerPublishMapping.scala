package org.apache.spark.sql.itsumma.gpconnector

final case class PortRange(start: Int, end: Int) {
  require(start >= 1 && start <= 65535, s"Invalid port range start: $start")
  require(end >= 1 && end <= 65535, s"Invalid port range end: $end")
  require(start <= end, s"Invalid port range: $start-$end")

  def ports: Iterator[Int] = (start to end).iterator
  def size: Int = end - start + 1
  def contains(port: Int): Boolean = port >= start && port <= end

  override def toString: String = s"$start-$end"
}

final case class PublishEndpoint(
    publishHost: String,
    publishPortRange: PortRange,
    localPortRange: PortRange) {
  require(
    publishPortRange.size == localPortRange.size,
    s"Publish port range $publishPortRange and local port range $localPortRange " +
      "must contain the same number of ports"
  )

  def publishPortFor(localPort: Int): Int = {
    require(
      localPortRange.contains(localPort),
      s"Local port $localPort is outside configured range $localPortRange"
    )
    publishPortRange.start + localPort - localPortRange.start
  }
}

final case class PublishBinding(localHost: String, endpoint: Option[PublishEndpoint]) {
  def publishHost: String = endpoint.map(_.publishHost).getOrElse(localHost)
  def publishPortRange: Option[PortRange] = endpoint.map(_.publishPortRange)
  def localPortRange: Option[PortRange] = endpoint.map(_.localPortRange)
  def publishPortFor(localPort: Int): Int = endpoint.map(_.publishPortFor(localPort)).getOrElse(localPort)
}

object ServerPublishMapping {
  val OptionName = "server.publish.mapping"

  def parse(raw: String): Map[String, PublishEndpoint] = {
    val trimmed = Option(raw).getOrElse("").trim
    if (trimmed.isEmpty) {
      return Map.empty
    }

    val entries = trimmed.split(",").map(_.trim).filter(_.nonEmpty)
    entries.foldLeft(Map.empty[String, PublishEndpoint]) { (acc, entry) =>
      val parts = entry.split("=", 2).map(_.trim)
      if (parts.length != 2 || parts(0).isEmpty || parts(1).isEmpty) {
        throw invalidEntry(entry)
      }

      val localHost = parts(0)
      if (acc.contains(localHost)) {
        throw new IllegalArgumentException(s"Duplicate $OptionName entry for local host '$localHost'")
      }

      val mappingParts = parts(1).split("->", -1).map(_.trim)
      if (mappingParts.length != 2 || mappingParts.exists(_.isEmpty)) {
        throw invalidEntry(entry)
      }

      val publishSpec = mappingParts(0)
      val colon = publishSpec.lastIndexOf(':')
      if (colon <= 0 || colon == publishSpec.length - 1) {
        throw invalidEntry(entry)
      }

      val publishHost = publishSpec.substring(0, colon).trim
      val publishRangeText = publishSpec.substring(colon + 1).trim
      if (publishHost.isEmpty) {
        throw invalidEntry(entry)
      }

      val publishPortRange = parsePortRange(publishRangeText, "publish", entry)
      val localPortRange = parsePortRange(mappingParts(1), "local", entry)
      acc + (localHost -> PublishEndpoint(publishHost, publishPortRange, localPortRange))
    }
  }

  private def parsePortRange(value: String, label: String, entry: String): PortRange = {
    val rangeParts = value.split("-", 2).map(_.trim)
    if (rangeParts.length != 2 || rangeParts.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Invalid $label port range '$value' in $OptionName entry '$entry'"
      )
    }
    PortRange(parsePort(rangeParts(0), label, entry), parsePort(rangeParts(1), label, entry))
  }

  private def parsePort(value: String, label: String, entry: String): Int = {
    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"Invalid $label port '$value' in $OptionName entry '$entry'"
        )
    }
  }

  private def invalidEntry(entry: String): IllegalArgumentException = {
    new IllegalArgumentException(
      s"Invalid $OptionName entry '$entry'. Expected " +
        "'<local-ip>=<publish-ip>:<publish-start>-<publish-end>-><local-start>-<local-end>'"
    )
  }
}
