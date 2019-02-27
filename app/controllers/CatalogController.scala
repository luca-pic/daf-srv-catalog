package controllers

import javax.inject.Inject
import play.api.{Configuration, Logger}
import play.api.libs.circe.Circe
import play.api.libs.ws.WSClient
import play.api.mvc._
import it.gov.daf.model._
import io.circe.generic.auto._
import io.swagger.annotations._
import it.gov.daf.exception._
import it.gov.daf.catalogmanager.catalog.MongoRepository
import it.gov.daf.catalogmanager.elasticsearch.ElasticsearchRepository
import it.gov.daf.catalogmanager.kafkaProxy.KafkaProxyRepository
import it.gov.daf.catalogmanager.kylo.KyloRepository
import it.gov.daf.catalogmanager.utils.CatalogManager
import it.gov.daf.common.authentication.Authentication
import it.gov.daf.common.config.ConfigReadException
import it.gov.daf.common.sso.common.CredentialManager
import it.gov.daf.common.utils.RequestContext.execInContext
import it.gov.daf.common.utils.UserInfo
import it.gov.daf.config._
import org.pac4j.play.store.PlaySessionStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

@Api
class CatalogController @Inject()(val playSessionStore: PlaySessionStore)
                                 (implicit ws: WSClient, configuration: Configuration) extends Controller with Circe {

  Authentication(configuration, playSessionStore)
  type DafSuccess = it.gov.daf.model.Success
  val DafResponseSuccess = it.gov.daf.model.Success

  implicit val localConfig: LocalConfig = LocalConfig.reader.read(configuration) match {
    case Failure(error) => throw ConfigReadException(s"Unable to read [local config]", error)
    case Success(config) => config
  }

  implicit val kafkaProxyConfig: KafkaProxyConfig = KafkaProxyConfig.reader.read(configuration) match {
    case Failure(error) => throw ConfigReadException(s"Unable to read [kafka proxy config]", error)
    case Success(config) => config
  }


  private val logger = Logger(this.getClass.getName)
  protected val mongoClient = new MongoRepository
  private val kafkaProxyClient = new KafkaProxyRepository
  private val kyloClient = new KyloRepository
  private val elasticsearchClient = new ElasticsearchRepository

  protected def getUserInfo[A](request: Request[A]) = {
    CredentialManager.readCredentialFromRequest(request)
  }

  @ApiOperation(value = "get public metacatalog from db", response = classOf[MetaCatalog])
  def getPublicCatalog(name: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("getPublicCatalog") { () =>
      handleException[MetaCatalog] {
        mongoClient.getPublicMetaCatalogByName(name)
      }
    }
  }

  @ApiOperation(value = "get metacatalog", response = classOf[MetaCatalog])
  def getCatalog(name: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("getCatalog") { () =>
      handleException[MetaCatalog] {
        val userInfo: UserInfo = getUserInfo[AnyContent](request)
        mongoClient.getPrivateMetaCatalogByName(name, userInfo.username, userInfo.groups.toList)
      }
    }
  }

  @ApiOperation(value = "check if the name is present on db", response = classOf[Boolean])
  def isPresent(name: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("isPresent") { () =>
      handleException[Boolean] {
        Future.successful(Right(mongoClient.isPresent(name)))
      }
    }
  }

  @ApiOperation(value = "get metacatalog by logical uri", response = classOf[MetaCatalog])
  def getCatalogByLogicalUri(logicalUri: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("getCatalogByLogicalUri") { () =>
      handleException[MetaCatalog] {
        Future.successful(mongoClient.getMetaCatalogByLogicalUri(logicalUri))
      }
    }
  }

  @ApiOperation(value = "add metacatalog on db", response = classOf[DafSuccess])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "MetaCatalog to save", name = "metacatalog",
      required = true, dataType = "it.gov.daf.model.MetaCatalog", paramType = "body")
  )
  )
  def addMetaCatalog = Action.async(circe.json[MetaCatalog]) { implicit request =>
    execInContext[Future[Result]]("addMetaCatalog") { () =>
      handleException[DafSuccess] {
        if (mongoClient.isPresent(request.body.dcatapit.name)) Future.failed(NameAlredyExistException())
        else {
          request.body.dcatapit.owner_org match {
            case None => Future.failed(NoOrgDatasetException())
            case Some(org) =>
              if (CredentialManager.isOrgAdmin(request, org) || CredentialManager.isOrgEditor(request, org))
                mongoClient.addMetaCatalog(request.body)
              else
                Future.successful(Left(Error(Some(401), s"Unauthorized to add metacatalog for organization $org", None)))
          }
        }
      }
    }
  }

  @ApiOperation(value = "add metacalog to the kafka queue", response = classOf[DafSuccess])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "MetaCatalog to save", name = "metacatalog",
      required = true, dataType = "it.gov.daf.model.MetaCatalog", paramType = "body")
    )
  )
  def addQueueCatalog = Action.async(circe.json[MetaCatalog]) { implicit request =>
    execInContext[Future[Result]]("addQueueCatalog") { () =>
      handleException[DafSuccess] {
        val OptionToken = CatalogManager.readTokenFromRequest(request.headers, false)
        OptionToken match {
          case None => logger.debug("Not token found"); Future.successful(Left(Error(Some(401), s"Not token found", None)))
          case Some(token) =>
            val credentials = CredentialManager.readCredentialFromRequest(request)

            if (CredentialManager.isDafSysAdmin(request) || CredentialManager.isOrgsEditor(request, credentials.groups) ||
              CredentialManager.isOrgsAdmin(request, credentials.groups)) {
              kafkaProxyClient.sendMessageKafkaProxy(credentials.username, request.body, token)
            }
            else logger.debug("Unauthorized to add metacatalog, admin or editor permissions required");
            Future.successful(Left(Error(Some(401), s"Unauthorized to add metacatalog, admin or editor permissions required", None)))
        }
      }
    }
  }

  @ApiOperation(value = "delete dataset", response = classOf[DafSuccess])
  def deleteCatalog(datasetName: String, org: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("deleteCatalog") { () =>
      handleException[DafSuccess] {
        def callDeleteFromCkanGeo(isPrivate: Boolean, datasetId: String): Future[Either[Error, DafSuccess]] = {
          Logger.logger.debug(s"try to delete $datasetId, isPrivate $isPrivate")
          if (!isPrivate) {
            Logger.logger.debug(s"$datasetId is public")
            ws.url(s"${localConfig.host}:${localConfig.port}/ckan/purgeDatasetCkanGeo/$datasetId").delete().map { res =>
              res.status match {
                case 200 => Logger.logger.debug(s"$datasetId deleted from ckan-geo"); Right(DafResponseSuccess(s"$datasetId deleted", None))
                case _ => Logger.logger.debug(s"error in delete $datasetId from ckan-geo: ${res.body}"); Left(Error(Some(res.status), "ckan-geo resp: " + res.body, None))
              }
            }
          }
          else Logger.logger.debug(s"$datasetId is private")
          Future.successful(Right(DafResponseSuccess("", None)))
        }

        def sendNotifications(user: String, datasetName: String, error: String, token: String): Unit = {
          //user notification
          kafkaProxyClient.sendGenericMessageToKafka(None, Some(user), "notification", "delete_error", s"Dataset $datasetName non cancellato", s"Non è stato possibile cancellare il dataset $datasetName, è stata contattata l'assistenza", None, token)
          //admin notification
          kafkaProxyClient.sendGenericMessageToKafka(Some(kafkaProxyConfig.dafAdminGroup), None, "notification", "delete_error", s"Dataset $datasetName non cancellato", error, None, token)
        }

        val credential = CredentialManager.readCredentialFromRequest(request)

        val isSysAdmin = CredentialManager.isDafSysAdmin(request)

        val user = credential.username

        val groups = credential.groups.toList

        val feedName = s"$org.${org}_o_$datasetName"

        val optionToken = CatalogManager.readTokenFromRequest(request.headers, false)

        if (optionToken.isDefined && (groups.contains(org) || isSysAdmin)) {
          mongoClient.internalCatalogByName(datasetName, user, org, isSysAdmin, optionToken.get, ws).flatMap {
            case Right(catalog) => {
              val globalResponse = kyloClient.deleteFeed(feedName, user) flatMap {
                case Right(_) => {
                  callDeleteFromCkanGeo(catalog.dcatapit.privatex.getOrElse(false), datasetName) flatMap {
                    case Right(_) => {
                      val datasetOwner = isSysAdmin match {
                        case false => user
                        case true => catalog.dcatapit.author.get
                      }
                      mongoClient.deleteCatalogByName(datasetName, datasetOwner, org, isSysAdmin, optionToken.get, ws)
                    }
                    case Left(error) => Future.successful(Left(error))
                  }
                }
                case Left(error) => Future.successful(Left(error))
              }
              globalResponse.flatMap {
                case Right(success) => Future.successful(Right(success))
                case Left(error) => sendNotifications(user, datasetName, error.message, optionToken.get); Future.successful(Left(error))
              }
            }
            case Left(_) => Future.successful(Left(Error(Some(404), s"catalog $datasetName not found", None)))
          }
        }
        else Future.successful(Left(Error(Some(401), s"Unauthorized to delete dataset $datasetName", None)))
      }
    }
  }

  @ApiOperation(value = "get standard fields", response = classOf[Seq[DatasetNameFields]])
  def getDatasetStandardFields = Action.async { implicit request =>
    execInContext[Future[Result]]("getDatasetStandardFields") { () =>
      handleException[Seq[DatasetNameFields]] {
        val credentials = CredentialManager.readCredentialFromRequest(request)
        mongoClient.getDatasetStandardFields(credentials.username, credentials.groups.toList)
      }
    }
  }

  @ApiOperation(value = "get all tags", response = classOf[Seq[String]])
  def getTags = Action.async { implicit request =>
    execInContext[Future[Result]]("getTags") { () =>
      handleException[Seq[String]] {
        elasticsearchClient.getTag
      }
    }
  }

  @ApiOperation(value = "return linked dataset", response = classOf[Seq[LinkedDataset]])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "get linked dataset", name = "linkedParams",
      required = true, dataType = "it.gov.daf.model.LinkedParams", paramType = "body")
    )
  )
  def getLinkedDataset(datasetName: String, limit: Option[Int]) = Action.async(circe.json[LinkedParams]) { implicit request =>
    execInContext[Future[Result]]("getLinkedDataset") { () =>
      handleException[Seq[LinkedDataset]] {
        val credentials = CredentialManager.readCredentialFromRequest(request)
        elasticsearchClient.getLinkedDatasets(datasetName, request.body, credentials.username, credentials.groups.toList, limit)
      }
    }
  }

  @ApiOperation(value = "send message to kafka", response = classOf[DafSuccess])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "message info to send to kafka", name = "kafkaMsgInfo",
      required = true, dataType = "it.gov.daf.model.KafkaMessageInfo", paramType = "body")
    )
  )
  def sendToKafka = Action.async(circe.json[KafkaMessageInfo]) { implicit request =>
    execInContext[Future[Result]]("sendToKafka") { () =>
      handleException[DafSuccess] {
        val optionToke: Option[String] = CatalogManager.readTokenFromRequest(request.headers, false)
        val kafkaMsgInfo = request.body
        optionToke match {
          case Some(t) => {
            kafkaProxyClient.sendGenericMessageToKafka(
              kafkaMsgInfo.group,
              kafkaMsgInfo.user,
              kafkaMsgInfo.topicName,
              kafkaMsgInfo.notificationType,
              kafkaMsgInfo.title,
              kafkaMsgInfo.description,
              kafkaMsgInfo.link,
              t)
          }
          case None => Future.successful(Left(Error(Some(401), "need Bearer token", None)))
        }
      }
    }
  }


  @ApiOperation(value = "list of fields name of vocabulary", response = classOf[Seq[DatasetNameFields]])
  def getFieldsVoc() = Action.async { implicit request =>
    execInContext[Future[Result]]("getFieldsVoc") { () =>
      handleException[Seq[DatasetNameFields]] {
        mongoClient.getFieldsVoc
      }
    }
  }
}
