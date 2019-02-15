package controllers

import java.io.FileInputStream
import java.net.URLEncoder

import io.swagger.annotations.{ApiImplicitParam, ApiImplicitParams, ApiOperation}
import javax.inject.Inject
import org.pac4j.play.store.PlaySessionStore
import play.api.{Configuration, Logger}
import play.api.libs.circe.Circe
import io.circe.generic.auto._
import it.gov.daf.catalogmanager.kylo.{KyloRepository, KyloTrasformers}
import it.gov.daf.common.authentication.Authentication
import it.gov.daf.common.config.ConfigReadException
import it.gov.daf.common.sso.common.CredentialManager
import it.gov.daf.common.utils.RequestContext.execInContext
import it.gov.daf.config.{KyloConfig, ServicesConfig}
import play.api.libs.ws.{WSAuthScheme, WSClient, WSRequest, WSResponse}
import play.api.mvc.{Action, Controller, Result}
import it.gov.daf.model.{Error, InputSrc, MetaCatalog}
import play.Environment
import play.api.libs.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class KyloController @Inject()(val playSessionStore: PlaySessionStore)
                              (implicit ws: WSClient, configuration: Configuration) extends Controller with Circe {

  Authentication(configuration, playSessionStore)

  type DafSuccess = it.gov.daf.model.Success
  val DafResponseSuccess = it.gov.daf.model.Success

  private val logger = Logger(this.getClass.getName)
  private val kylo = new KyloRepository

  implicit val kyloConfig: KyloConfig = KyloConfig.reader.read(configuration) match {
    case Failure(error) => throw ConfigReadException(s"Unable to read [kylo config]", error)
    case Success(config) => config
  }

  implicit val services: ServicesConfig = ServicesConfig.reader.read(configuration) match {
    case Failure(error) => throw ConfigReadException(s"Unable to read [services config]", error)
    case Success(config) => config
  }

  private val KYLOUSER = kyloConfig.user
  private val KYLOURL = kyloConfig.url
  private val KYLOPWD = kyloConfig.userPwd


  @ApiOperation(value = "create a kylo feed", response = classOf[DafSuccess])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "MetaCatalog to save", name = "metacatalog",
      required = true, dataType = "it.gov.daf.model.MetaCatalog", paramType = "body")
    )
  )
  def createKyloFeed(fileType: String) = Action.async(circe.json[MetaCatalog]) { implicit request =>
    execInContext[Future[Result]]("createKyloFeed") { () =>
      handleException[DafSuccess] {

        val feed: MetaCatalog = request.body

        if (feed.operational.type_info.isDefined && feed.operational.type_info.get.dataset_type.equals("derived_sql")) {
          logger.info("create feed started")

          val streamKyloTemplate = new FileInputStream(Environment.simple().getFile("/data/kylo/template_trasformation.json"))

          val kyloTemplate: JsValue = try {
            Json.parse(streamKyloTemplate)
          } finally {
            streamKyloTemplate.close()
          }

          val categoryFuture: Future[JsValue] = kylo.categoryFuture(feed)
          val kyloSchema: String = feed.dataschema.kyloSchema.get
          val inferJson: JsValue = Json.parse(kyloSchema)

          val feedCreation: WSRequest = ws.url(KYLOURL + "/api/v1/feedmgr/feeds")
            .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)


          val feedData: Future[JsResult[JsObject]] = for {
            category <- categoryFuture
            trasformed <- Future(kyloTemplate.transform(
              KyloTrasformers.feedTrasformationTemplate(feed,
                category))
            )
          } yield trasformed

          val createFeed: Future[WSResponse] = feedData.flatMap {
            case s: JsSuccess[JsValue] => logger.debug(Json.stringify(s.get)); feedCreation.post(s.get)
            case e: JsError => throw new Exception(JsError.toJson(e).toString())
          }

          createFeed onComplete (r => Logger.logger.debug(s"kyloResp: ${r.get.status}"))

          val result: Future[Either[Error, DafSuccess]] = createFeed.flatMap {
            // Assuming status 200 (OK) is a valid result for you.
            case resp: WSResponse if resp.status == 200 => logger.debug(Json.stringify(resp.json)); Future.successful(Right(DafResponseSuccess("Feed started", Option(resp.body))))
            case _ => Future.successful(Left(Error(Some(401), "Feed not created", None)))
          }

          result

        } else {


          val skipHeader: Boolean = fileType match {
            case "csv" => true
            case "json" => false
          }


          val ingest: String = feed.operational.input_src match {
            case InputSrc(Some(_), None, None, _) => "sftp"
            case InputSrc(None, Some(_), None, _) => "srv_pull"
            case InputSrc(None, None, Some(_), _) => "srv_push"
          }

          val user: String = CredentialManager.readCredentialFromRequest(request).username

          val domain: String = feed.operational.theme
          val subDomain: String = feed.operational.subtheme
          val dsName: String = feed.dcatapit.name


          val path: String = ingest match {
            case "srv_push" => s"/uploads/$user/$domain/$subDomain/$dsName"
            case _ => s"/home/$user/ftp/$domain/$subDomain/$dsName"
          }

          logger.debug(s"$ingest: $path")


          val templateProperties: Future[(JsValue, List[JsObject])] = ingest match {
            case "sftp" => kylo.sftpRemoteIngest(fileType, path)
            case "srv_pull" => kylo.wsIngest(fileType, feed)
            case "srv_push" => kylo.hdfsIngest(fileType, path)
          }

          val categoryFuture: Future[JsValue] = kylo.categoryFuture(feed)

          val streamKyloTemplate = new FileInputStream(Environment.simple().getFile("/data/kylo/template_test.json"))

          val kyloTemplate: JsValue = try {
            Json.parse(streamKyloTemplate)
          } finally {
            streamKyloTemplate.close()
          }

          val sftpPath = URLEncoder.encode(s"$domain/$subDomain/$dsName", "UTF-8")

          //            val createDir = ws.url("http://security-manager.default.svc.cluster.local:9000/security-manager/v1/sftp/init/" + URLEncoder.encode(sftpPath, "UTF-8") + s"?orgName=${feed.dcatapit.owner_org.get}")
          val createDir = ws.url(services.securityUrl + "/security-manager/v1/sftp/init/" + sftpPath + s"?orgName=${feed.dcatapit.owner_org.get}")
            .withHeaders(("authorization", request.headers.get("authorization").get))

          val kyloSchema = feed.dataschema.kyloSchema.get
          val inferJson = Json.parse(kyloSchema)

          val feedCreation = ws.url(KYLOURL + "/api/v1/feedmgr/feeds")
            .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)

          val feedData = for {
            (template, templates) <- templateProperties
            created <- createDir.get()
            category <- categoryFuture
            trasformed <- Future(kyloTemplate.transform(
              KyloTrasformers.feedTrasform(feed,
                template,
                templates,
                inferJson,
                category,
                fileType,
                skipHeader)
            )
            )
          } yield trasformed

          val createFeed: Future[WSResponse] = feedData.flatMap {
            case s: JsSuccess[JsValue] => logger.debug(Json.stringify(s.get)); feedCreation.post(s.get)
            case e: JsError => throw new Exception(JsError.toJson(e).toString())
          }

          createFeed onComplete (r => Logger.logger.debug(s"kyloResp: ${r.get.status}"))

          val result = createFeed.flatMap {
            // Assuming status 200 (OK) is a valid result for you.
            case resp: WSResponse if resp.status == 200 => logger.debug(Json.stringify(resp.json)); Future.successful(Right(DafResponseSuccess("Feed started", Option(resp.body))))
            case _ => Future.successful(Left(Error(Some(401), "Feed not created", None)))
          }

          result
        }
      }
    }
  }
}