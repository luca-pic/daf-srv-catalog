package controllers

import io.swagger.annotations.ApiOperation
import it.gov.daf.common.config.ConfigReadException
import it.gov.daf.common.sso.common.{CredentialManager, LoginInfo, SecuredInvocationManager}
import it.gov.daf.common.utils.RequestContext
import it.gov.daf.config.CkanGeoConfig
import javax.inject.Inject
import play.api.{Configuration, Logger}
import io.swagger.annotations._
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.mvc.{Action, Controller, Result}
import io.circe.generic.auto._
import it.gov.daf.catalogmanager.ckan.CkanGeoRepository
import it.gov.daf.common.authentication.Authentication
import play.api.libs.circe._

import scala.concurrent.Future
import it.gov.daf.model.{Dataset, Error}
import org.pac4j.play.store.PlaySessionStore
import play.api.libs.json.JsString

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class CkanController @Inject()(secInvokManager: SecuredInvocationManager, playSessionStore: PlaySessionStore, ws: WSClient, configuration: Configuration) extends Controller with Circe {

  Authentication(configuration, playSessionStore)

  type DafSuccess = it.gov.daf.model.Success
  val DafResponseSuccess = it.gov.daf.model.Success

  implicit val ckanGeoConfig: CkanGeoConfig = CkanGeoConfig.reader.read(configuration) match {
    case Success(config) => config
    case Failure(error) => throw ConfigReadException(s"Unable to read [ckan-geo config]", error)
  }

  private val CKAN_GEO_HOST = ckanGeoConfig.host
  private val CKAN_GEO_PORT = ckanGeoConfig.port
  private val CKAN_GEO_URL = s"$CKAN_GEO_HOST:$CKAN_GEO_PORT"
  private val logger = Logger(this.getClass.getName)

  private val ckanGeoClient = new CkanGeoRepository


  @ApiOperation(value = "purge dataset from ckan-geo", response = classOf[DafSuccess])
  def purgeDatasetCkanGeo(datasetId: String) = Action.async { implicit request =>
    RequestContext.execInContext[Future[Result]]("purgeDatasetCkanGeo") { () =>
      handleException[DafSuccess] {
        logger.debug(s"url ckan-geo $CKAN_GEO_URL")

        val user = CredentialManager.readCredentialFromRequest(request).username

        logger.debug(s"$user try to delete $datasetId")

        def callDeleteDataset(cookie: String, wsClient: WSClient): Future[WSResponse] = {
          wsClient.url(CKAN_GEO_URL + "/api/3/action/dataset_purge").withHeaders("Cookie" -> cookie).post(s"""{\"id\":\"$datasetId\"}""")
        }

        secInvokManager.manageServiceCall(new LoginInfo(null, null, "ckan-geo"), callDeleteDataset) map { r =>
          if (r.status == 200)
            logger.debug(s"$user deleted $datasetId")
          else
            logger.debug(s"$user not deleted $datasetId: ${(r.json \ "error" \ "message").getOrElse(JsString("no error message"))}")
          r.status match {
            case 200 => Right(DafResponseSuccess(s"$user deleted $datasetId", None))
            case _   => Left(Error(Some(r.status), r.body, None))
          }
        }
      }
    }
  }

  @ApiOperation(value = "add catalog to ckan-geo", response = classOf[DafSuccess])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "Dataset to save", name = "dcatapit",
      required = true, dataType = "it.gov.daf.model.Dataset", paramType = "body")
  )
  )
  def addCatalogCkanGeo: Action[Dataset] = Action.async(circe.json[Dataset]) { implicit request =>
    RequestContext.execInContext[Future[Result]]("purgeDatasetCkanGeo") { () =>
      handleException[DafSuccess] {

        val user: String = CredentialManager.readCredentialFromRequest(request).username

        val jsonString: String = ckanGeoClient.createCkanGeoCatalog(request.body)

        logger.debug(s"$user try to save: $jsonString")

        def callCreateDataset(cookie: String, wsClient: WSClient): Future[WSResponse] = {
          wsClient.url(CKAN_GEO_URL + "/api/3/action/package_create").withHeaders("Cookie" -> cookie).post(jsonString)
        }

        secInvokManager.manageServiceCall(new LoginInfo(user, null, "ckan-geo"), callCreateDataset) map { r =>
          if (r.status == 200)
            logger.debug(s"$user added ${request.body.name}")
          else
            logger.debug(s"$user error in add ${request.body.name}: ${(r.json \ "error").getOrElse(JsString("no error message"))}")
          r.status match {
            case 200 => Right(DafResponseSuccess(s"$user added ${request.body.name}", None))
            case _ => Left(Error(Some(r.status), r.body, None))
          }
        }
      }
    }
  }


  @ApiOperation(value = "add catalog to ckan-geo", response = classOf[String])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "Dataset to save", name = "dcatapit",
      required = true, dataType = "it.gov.daf.model.Dataset", paramType = "body")
  )
  )
  def test = Action.async(circe.json[Dataset]) { implicit request =>
    RequestContext.execInContext[Future[Result]]("test") { () =>
      handleException[String] {
        val k = ckanGeoClient.createCkanGeoCatalog(request.body)

        println(k)

        Future.successful(Right("ok"))
      }
    }
  }
}