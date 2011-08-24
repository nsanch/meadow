package com.foursquare.meadow

import com.mongodb.{BasicDBObject, WriteConcern}
import org.bson.BSONObject
import org.junit.Assert._
import org.junit.Test

class MongoTest {
  @Test
  def testSaveAndLoad: Unit = {
    val created = SampleSchema.createRecord
    val createdId = created._id.get
    created.int.set(77)

    SampleSchema.save(created)

    val foundSampleOpt = SampleSchema.findOne(createdId)
    val foundSample = foundSampleOpt.get
    assertEquals(created.id, foundSample.id)
    assertEquals(created.int.getOpt, foundSample.int.getOpt)
  }
}
