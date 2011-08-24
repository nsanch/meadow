package com.foursquare.meadow

import com.foursquare.meadow.Implicits._
import com.mongodb.BasicDBObject
import org.junit.Test
import org.junit.Assert._

class ReuseTest {
  @Test
  def verifyCleared: Unit = {
    val sample = SampleSchema.createRecord
    sample.int.set(1)
    val firstId = sample._id.get
    SampleSchema.save(sample) 

    sample.clearForReuse

    assertEquals(None, sample.int.getOpt)
    assertEquals(None, sample._id.getOpt)

    // reinit as a new record 
    sample.init(new BasicDBObject, true)
    val secondId = sample._id.get
    assertFalse(firstId =? secondId)
  }


  @Test
  def testFreelist: Unit = {
    val first = FreelistedRecSchema.createRecord
    FreelistedRecSchema.releaseRecord(first)
    val second = FreelistedRecSchema.createRecord
    
    assertSame(first, second)
  }
}
