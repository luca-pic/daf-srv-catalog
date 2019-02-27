package it.gov.daf.catalogmanager.catalog

import com.mongodb
import com.mongodb.{DBObject, ServerAddress}
import com.mongodb.casbah.{MongoClient, MongoCollection, MongoCredential}
import it.gov.daf.config.{MongoConfig, ServicesConfig}
import javax.inject.Inject
import play.api.{Configuration, Logger}
import it.gov.daf.model._
import io.circe.parser._
import io.circe.generic.auto._
import it.gov.daf.catalogmanager.utils.CatalogManager
import mongodb.casbah.query.Imports._
import io.circe.syntax._
import it.gov.daf.common.config.ConfigReadException
import it.gov.daf.model
import play.api.libs.ws.WSClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class MongoRepository @Inject()(implicit configuration: Configuration) {

  val servicesConfig: ServicesConfig = ServicesConfig.reader.read(configuration) match {
        case Failure(error) => throw ConfigReadException(s"Unable to read [services config]", error)
        case Success(config) => config
  }

  val mongoConfig: MongoConfig = MongoConfig.reader.read(configuration) match {
    case Success(config) => config
    case Failure(error) => throw ConfigReadException(s"Unable to read [mongo config]", error)
  }

  type DafSuccess = it.gov.daf.model.Success
  val DafResponseSuccess: model.Success.type = it.gov.daf.model.Success

  private val mongoHost = mongoConfig.host
  private val mongoPort = mongoConfig.port
  private val databaseName = mongoConfig.database
  private val collectionName = mongoConfig.collection
  private val userName = mongoConfig.username
  private val password = mongoConfig.password

  private val server = new ServerAddress(mongoHost, 27017)
  private val credentials = MongoCredential.createCredential(userName, databaseName, password.toCharArray)

  protected val collection: MongoCollection = MongoClient(server, List(credentials))(databaseName)(collectionName)

  private val logger = Logger(this.getClass.getName)

  private def createPublicQuery(name: String) = {
    import mongodb.casbah.query.Imports.$and
    $and(
      com.mongodb.casbah.commons.Imports.MongoDBObject("dcatapit.name" -> name),
      com.mongodb.casbah.commons.Imports.MongoDBObject("dcatapit.privatex" -> false))
  }

  private def createPrivateQuery(name: String, user: String, groups: List[String]) = {
    $or(
      $and(
        MongoDBObject("dcatapit.name" -> name),
        $or(
          MongoDBObject("dcatapit.author" -> user),
          "operational.acl.groupName" $in groups
        )
      ),
      createPublicQuery(name)
    )
  }

  def getPublicMetaCatalogByName(name :String): Future[Either[Error, MetaCatalog]] = {
    val query = createPublicQuery(name)
    val result = collection.findOne(query)
    val response = result match {
      case Some(mongoResponse) => {
        val jsonString = com.mongodb.util.JSON.serialize(mongoResponse)
        val decodeMetacatalog = decode[MetaCatalog](jsonString)
        decodeMetacatalog match {
          case Left(error)        => logger.debug(s"error in parsing response from mongo: ${error.getCause}"); Left(Error(Some(500), error.getMessage, None))
          case Right(metacatalog) => logger.debug(s"found dataset $name"); Right(metacatalog)
        }
      }
      case None => logger.debug(s"$name not found"); Left(Error(Some(404), s"$name not found", None))
    }
    Future.successful(response)
  }

  def getPrivateMetaCatalogByName(name: String, user: String, groups: List[String]): Future[Either[Error, MetaCatalog]] = {
    val query = createPrivateQuery(name, user, groups)
    val result = collection.findOne(query)
    val response = result match {
      case Some(mongoResponse) => {
        val jsonString = com.mongodb.util.JSON.serialize(mongoResponse)
        val decodeMetacatalog = decode[MetaCatalog](jsonString)
        decodeMetacatalog match {
          case Left(error)        => logger.debug(s"error in parsing response from mongo: ${error.getCause}"); Left(Error(Some(500), error.getMessage, None))
          case Right(metacatalog) => logger.debug(s"found dataset $name"); Right(metacatalog)
        }
      }
      case None => logger.debug(s"$name not found"); Left(Error(Some(404), s"$name not found", None))
    }
    Future.successful(response)
  }

  def isPresent(name: String): Boolean = {
    val query = MongoDBObject("dcatapit.name" -> name)
    val result = collection.findOne(query)
    result match {
      case Some(_) => true
      case _       => false
    }
  }

  def addMetaCatalog(metaCatalog: MetaCatalog): Future[Either[Error, DafSuccess]] = {
    val res = metaCatalog.operational.std_schema match {
      case Some(std_schema) => {
        getMetaCatalogByLogicalUri(std_schema.std_uri) match {
          case Left(error) => logger.debug(s"error in get standard metacatalog: $error"); Left(error)
          case Right(stdCatalog) => {
            val res = CatalogManager.writeOrdinaryWithStandard(metaCatalog, stdCatalog)
            res match {
              case Some(meta) => {
                val obj = com.mongodb.util.JSON.parse(meta.asJson.toString()).asInstanceOf[DBObject]
                val responseInsert = collection.insert(obj)
                if(responseInsert.wasAcknowledged()) { logger.debug(s"${meta.dcatapit.name} inserted"); Right(DafResponseSuccess(s"${meta.dcatapit.name} inserted", None)) }
                else { logger.debug(s"error in insert ${meta.dcatapit.name}"); Left(Error(Some(500), "Internal server error", None)) }
              }
              case None => logger.debug(s"error in writeOrdinaryWithStandard for metacatalog ${metaCatalog.dcatapit.name}"); Left(Error(Some(500), "Internal server error", None))
            }
          }
        }
      }
      case None => {
        CatalogManager.writeOrdAndStdOrDerived(metaCatalog) match {
          case None    => logger.debug(s"error in writeOrdAndStdOrDerived for metacatalog ${metaCatalog.dcatapit.name}"); Left(Error(Some(500), "Internal server error", None))
          case Some(meta) => {
            val obj = com.mongodb.util.JSON.parse(meta.asJson.toString()).asInstanceOf[DBObject]
            val responseInsert = collection.insert(obj)
            if(responseInsert.wasAcknowledged()) { logger.debug(s"${meta.dcatapit.name} inserted"); Right(DafResponseSuccess(s"${meta.dcatapit.name} inserted", None)) }
            else { logger.debug(s"error in insert ${meta.dcatapit.name}"); Left(Error(Some(500), "Internal server error", None)) }
          }
        }
      }
    }
    Future.successful(res)
  }

  def getMetaCatalogByLogicalUri(logicalUri: String) = {
    val query = MongoDBObject("operational.logical_uri" -> logicalUri)
    val result = collection.findOne(query)
    result match {
      case Some(mongoResponse) => {
        val jsonString = com.mongodb.util.JSON.serialize(mongoResponse)
        val decodeMetacatalog = decode[MetaCatalog](jsonString)
        decodeMetacatalog match {
          case Left(error)        => logger.debug(s"error in parsing response from mongo: ${error.getMessage}"); Left(Error(Some(500), error.getMessage, None))
          case Right(metacatalog) => logger.debug(s"found dataset $logicalUri"); Right(metacatalog)
        }
      }
      case None => logger.debug(s"$logicalUri not found"); Left(Error(Some(404), s"$logicalUri not found", None))
    }
  }

  private def canDeleteCatalog(isSysAdmin: Boolean, name: String, token: String, wsClient: WSClient, user: String) = {
    val url = if(isSysAdmin) "/dati-gov/v1/dashboard/iframesByName/" + name + s"?user=$user" else "/dati-gov/v1/dashboard/iframesByName/" + name

    val widgetsResp = wsClient.url(servicesConfig.datipubbliciUrl + url)
      .withHeaders("Authorization" -> s"Bearer $token")
      .get()

    widgetsResp.map{ res =>
      if(res.status == 200 && !res.body.equals("[]")) { Logger.debug(s"$name has some widgets"); false }
      else if(res.status == 200) { Logger.debug(s"$name not has some widgets"); true }
      else { Logger.debug(s"$name: Internal server error"); false }
    }
  }

  private def createQueryToDeleteCatalog(isSysAdmin: Boolean, name: String, user: String, org: String) = {
    import mongodb.casbah.query.Imports._

    isSysAdmin match {
      case false => $and(MongoDBObject("dcatapit.name" -> name), MongoDBObject("dcatapit.author" -> user), MongoDBObject("dcatapit.owner_org" -> org), "operational.acl.groupName" $exists false)
      case true  => $and(MongoDBObject("dcatapit.name" -> name), MongoDBObject("dcatapit.owner_org" -> org), "operational.acl.groupName" $exists false)
    }
  }

  def internalCatalogByName(name: String, user: String, org: String, isSysAdmin: Boolean, token: String, wsClient: WSClient): Future[Either[Error, MetaCatalog]] = {
    canDeleteCatalog(isSysAdmin, name, token, wsClient, user).map{ booleanResp =>
      if(booleanResp) {
        val query = createQueryToDeleteCatalog(isSysAdmin, name, user, org)
        val result = collection.findOne(query)
        result match {
          case Some(catalog) => {
            val jsonString = com.mongodb.util.JSON.serialize(catalog)
            val decodeMetacatalog = decode[MetaCatalog](jsonString)
            decodeMetacatalog match {
              case Left(error)        => logger.debug(s"$name validation error: $error"); Left(Error(Some(500), s"Internal server error", None))
              case Right(metacatalog) => logger.debug(s"$name found"); Right(metacatalog)
            }
          }
          case _ => Left(Error(Some(404), s"$name not found", None))
        }
      }
      else Left(Error(Some(403), s"is not possible delete catalog $name, it has some widgets", None))
    }
  }

  def deleteCatalogByName(nameCatalog: String, user: String, org: String, isSysAdmin: Boolean, token: String, wsClient: WSClient): Future[Either[Error, DafSuccess]] = {

    Logger.logger.debug(s"$user try to delete $nameCatalog")

    canDeleteCatalog(isSysAdmin, nameCatalog, token, wsClient, user).map{ booleanResp =>
      if(booleanResp){
        val query = createQueryToDeleteCatalog(isSysAdmin, nameCatalog, user, org)
        val result = if(collection.remove(query).getN > 0) Right(DafResponseSuccess(s"catalog $nameCatalog deleted", None)) else Left(Error(Some(404), s"catalog $nameCatalog not found", None))
        Logger.logger.debug(s"$nameCatalog deleted from catalog_test result: ${result.isRight}")
        result
      } else { Logger.logger.debug("mongo: connection error");Left(Error(Some(500), s"connection error", None)) }
    }
  }

  def getDatasetStandardFields(user: String, groups: List[String]): Future[Either[Error, Seq[DatasetNameFields]]] = {
    import mongodb.casbah.query.Imports._

    def parseResponse(seqMetacatalog: Seq[MetaCatalog]) = {
      seqMetacatalog.map{catalog => DatasetNameFields(catalog.dcatapit.name, catalog.dataschema.avro.fields.get.map(f => f.name))}
    }

    val query = $and(
      $or(MongoDBObject("dcatapit.author" -> user), "operational.acl.groupName" $in groups),
      MongoDBObject("operational.is_std" -> true)
    )
    val results = collection.find(query).toList
    val jsonString = com.mongodb.util.JSON.serialize(results)
    val decodeSeqMetacatalog = decode[Seq[MetaCatalog]](jsonString)
    decodeSeqMetacatalog match {
      case Left(error)                                     => logger.debug(s"error in get standard fields: $error"); Future.successful(Left(Error(Some(500), error.getMessage, None)))
      case Right(seqMetacatalog) if seqMetacatalog.isEmpty => logger.debug("standard fields not found"); Future.successful(Left(Error(Some(404), "not found", None)))
      case Right(seqMetacatalog)                           => logger.debug(s"found ${seqMetacatalog.size} standard catalog"); Future.successful(Right(parseResponse(seqMetacatalog)))
    }
  }

  def getFieldsVoc: Future[Either[Error, Seq[DatasetNameFields]]] = {
    def parseFieldsVoc(seqMetacatalog: Seq[MetaCatalog]) = seqMetacatalog.map{ catalog => DatasetNameFields(catalog.dcatapit.name, catalog.dataschema.avro.fields.get.map(f => f.name))}

    val result = collection.find(MongoDBObject("operational.is_vocabulary" -> true)).toList
    val jsonString = com.mongodb.util.JSON.serialize(result)
    val decodeSeqMetacatalog = decode[Seq[MetaCatalog]](jsonString)
    decodeSeqMetacatalog match {
      case Left(error)                                     => logger.debug(s"error in get voc: $error"); Future.successful(Left(Error(Some(500), error.getMessage, None)))
      case Right(seqMetacatalog) if seqMetacatalog.isEmpty => logger.debug("voc not found"); Future.successful(Left(Error(Some(404), "not found", None)))
      case Right(seqMetacatalog)                           => logger.debug(s"found ${seqMetacatalog.size} voc"); Future.successful(Right(parseFieldsVoc(seqMetacatalog)))
    }
  }
}