package instance

import akka.actor.ActorSystem

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import scala.util.{Failure, Success, Try}

trait AkkaInstance {
  this: ConfigurationInstance =>

  protected val systemName = s"TestSystem_${System.currentTimeMillis}"

  protected lazy val actorSystem = ActorSystem(systemName, configuration.underlying)

  def terminateAkka() = Try {
    Await.result(actorSystem.terminate(), 20.seconds)
  } match {
    case Success(_) => // nothing to do
    case Failure(error: TimeoutException) => throw error
    case Failure(error) => throw new RuntimeException("Unable to terminate actor system", error)
  }

  def startAkka(): Unit = {
    actorSystem.registerOnTermination {
      println("Actor System stopped")
    }
  }

}
