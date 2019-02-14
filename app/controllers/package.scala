
import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits.defaultContext

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import io.circe.Encoder
import io.circe.syntax._
import it.gov.daf.model.Error
import play.api.Logger



package object controllers {

  def handleException[A: Encoder](f: => Future[Either[Error, A]]): Future[Result] = {

    def wrapErrorResponse(error: Error) = {
      error.code match {
        case Some(400) => Results.BadRequest(error.message)
        case Some(401) => Results.Unauthorized(error.message)
        case Some(404) => Results.NotFound(error.message)
        case Some(500) => Results.InternalServerError(error.message)
        case _         => Results.InternalServerError
      }
    }
    f map {
      case Right(xx) => Results.Ok(xx.asJson.noSpaces).as("application/json")
      case Left(ee) => Logger.debug(s"error: $ee"); wrapErrorResponse(ee)
    }
  }

}

