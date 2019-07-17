package it.gov.daf.catalogmanager.utilities


import catalog_manager.yaml.{Error, Success}
import cats.data.EitherT
import play.api.Logger

import scala.concurrent.Future
import scala.util.{Failure, Try}
import scala.concurrent.ExecutionContext.Implicits._
import cats.implicits._


case class ErrorWrapper(error: Error, steps:Int)
case class SuccessWrapper[T](step: Int, success: T)
case class NifiRevisionObject(clientId: String, version: Int)



object ProcessHandler {

  def step[S](tryresp: => Future[Either[Error, S]]): EitherT[Future, ErrorWrapper, (S, Int)] = step(0, tryresp)

  def step[S](step: Int, fx: => Future[Either[Error, S]]): EitherT[Future, ErrorWrapper, (S, Int)] = {

    handleTries(step, fx) { a: S =>
      val newStep: Int = step + 1
      Right(a, newStep)
    }
  }

  private def handleTries[S, T](step: Int, fx: => Future[Either[Error, S]])(ff: S => Right[ErrorWrapper, T]): EitherT[Future, ErrorWrapper, T] = {


    Logger.logger.debug(s"handleTries step: $step")

    val out: Future[Either[ErrorWrapper, T]] =
      Try {
        fx.map {
          case Left(l) => val err = ErrorWrapper(l, step)
            Logger.logger.warn(err.toString)
            scala.util.Success(Left(err))

          case Right(r) => scala.util.Success(ff(r))

        }.recover { case e: Throwable => scala.util.Failure(e) } map {
          case scala.util.Success(resp) => resp
          case scala.util.Failure(f) => Logger.logger.error(s"Future Failure (step=$step) :${f.getMessage}", f)
            Left(ErrorWrapper(Error(f.getMessage, Option(0), None), step))
        }
      } match {
        case scala.util.Success(resp) => resp
        case scala.util.Failure(f) => Logger.logger.error(s"Failure (step=$step) :${f.getMessage}", f)
          Future.successful {
            Left(ErrorWrapper(Error(f.getMessage, Some(0), None), step))
          }
      }

    EitherT(out)
  }

}
