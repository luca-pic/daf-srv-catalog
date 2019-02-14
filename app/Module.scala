import com.google.inject.{AbstractModule, Singleton}
import it.gov.daf.common.sso.client.LoginClientRemote
import it.gov.daf.common.sso.common.{CacheWrapper, LoginClient}
import play.api.{Configuration, Environment, Logger}

@Singleton
class Module(environment: Environment, configuration: Configuration) extends AbstractModule{


  def configure(): Unit ={


    Logger.debug("executing module..")

    //bind[WSClient].toInstance("foo")
    //bind[GuiceSpec.type].toInstance(GuiceSpec)

//    bind(classOf[LoginClient]).to(classOf[LoginClientLocal])//for the initialization of SecuredInvocationManager
//
    val cacheWrapper = new CacheWrapper(Option(30L), Option(0L))// cookie 30 min, credential not needed
    bind(classOf[CacheWrapper]).toInstance(cacheWrapper)

    val securityManHost: Option[String] = configuration.getString("services.securityUrl")
    require(securityManHost.nonEmpty,"security.manager.host entry not provided")

    val loginClientRemote = new LoginClientRemote(securityManHost.get)
    bind(classOf[LoginClientRemote]).toInstance(loginClientRemote)
    bind(classOf[LoginClient]).to(classOf[LoginClientRemote])
  }

}


