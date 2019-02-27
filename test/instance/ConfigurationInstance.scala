package instance

import com.typesafe.config.ConfigFactory
import play.api.Configuration

trait ConfigurationInstance {

  protected val configFile = "test.conf"

  implicit val configuration: Configuration = Configuration { ConfigFactory.load(configFile) }

}
