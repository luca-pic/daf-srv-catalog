package controllers

import it.gov.daf.catalogmanager.nifi.NifiRepository
import it.gov.daf.common.authentication.Authentication
import javax.inject.Inject
import io.swagger.annotations._
import it.gov.daf.common.utils.RequestContext.execInContext
import io.circe.generic.auto._
import org.pac4j.play.store.PlaySessionStore
import play.api.libs.circe.Circe
import play.api.mvc.{Action, Controller, Result}
import play.api.{Configuration, Logger}
import play.api.libs.ws.WSClient

import scala.concurrent.Future

class NifiController @Inject()(val playSessionStore: PlaySessionStore)(implicit ws: WSClient, configuration: Configuration) extends Controller with Circe {

  Authentication(configuration, playSessionStore)

  private val logger = Logger(this.getClass.getName)
  private val nifiRepository = new NifiRepository

  type DafSuccess = it.gov.daf.model.Success

  @ApiOperation(value = "start nifi processor", response = classOf[DafSuccess])
  def startNifiProcessor(datasetName: String, orgName: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("startNifiProcessor") { () =>
      handleException[DafSuccess] {
        val feedName = s"${orgName}_o_${datasetName}"
        nifiRepository.startDerivedProcessor(orgName, feedName)
      }
    }
  }


}
