package com.foursquare.meadow

import com.mongodb.{Mongo, MongoOptions, ServerAddress}

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
