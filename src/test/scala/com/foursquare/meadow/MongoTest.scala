package com.foursquare.meadow

import com.mongodb.{BasicDBObject, WriteConcern}
import org.bson.BSONObject
import org.junit.Assert._
import org.junit.Test

class MongoTest {
  @Test
  def testSaveAndLoad: Unit = {
    val created = SampleDescriptor.createRecord
    val createdId = created._id.get
    created.int.set(77)

    created.save

    val foundSampleOpt = SampleDescriptor.findOne(createdId)
    val foundSample = foundSampleOpt.get
    assertEquals(created.id, foundSample.id)
    assertEquals(created.int.getOpt, foundSample.int.getOpt)
  }
}
