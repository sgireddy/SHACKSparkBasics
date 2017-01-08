package config

import com.typesafe.config.ConfigFactory
/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
object Settings {
  private val config = ConfigFactory.load()
  object ClickStreamGeneratorSettings {
    private val settings = config.getConfig("streamsettings")
    lazy val batchSize = settings.getInt("batch-size")
    lazy val productRangeStart = settings.getInt("product-range-start")
    lazy val productRangeEnd = settings.getInt("product-range-end")
    lazy val userRangeStart = settings.getInt("user-range-start")
    lazy val userRangeEnd = settings.getInt("user-range-end")
    lazy val promoAvailabilityFactor = settings.getInt("promo-availability-factor")
    lazy val streamDelay = settings.getInt("stream-delay-ms")
    lazy val tmpFile = settings.getString("tmp-file")
  }
}
