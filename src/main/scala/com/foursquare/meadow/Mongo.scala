package com.foursquare.meadow

import com.mongodb.{Mongo, MongoOptions, ServerAddress}

/**
 * Connects to Mongo. This is obviously only a toy connector, and should
 * instead be implemented by the caller of the library and passed into Meadow
 * somehow.
 */
object MongoConnector {
  lazy val mongo: Mongo = {
    val options = new MongoOptions()
    options.connectionsPerHost = 20
    options.threadsAllowedToBlockForConnectionMultiplier = 10
    options.maxWaitTime = 5000
    val mongo = new Mongo(new ServerAddress("localhost", 27017), options)
    mongo
  }
}
