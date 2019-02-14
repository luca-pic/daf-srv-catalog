package it.gov.daf.config

import it.gov.daf.common.config.Read

case class MongoConfig(host: String, port: Int, database: String, collection: String, username: String, password: String)

object MongoConfig {

  private def readConfig = Read.config { "mongo" }.!

  def readValues = for {
    host          <- Read.string { "host" }.!
    port          <- Read.int { "port" }.!
    database      <- Read.string { "database" }.!
    collection    <- Read.string { "collection" }.!
    username      <- Read.string { "username" }.!
    password      <- Read.string { "password" }.!
  } yield MongoConfig(
    host = host,
    port = port,
    database = database,
    collection = collection,
    username = username,
    password = password
  )

  def reader = readConfig ~> readValues

}

