package com.foursquare.meadow

import com.foursquare.meadow.Implicits._
import com.mongodb.BasicDBObject
import org.junit.Test
import org.junit.Assert._

class ReuseTest {
  @Test
  def verifyCleared: Unit = {
    val sample = SampleSchema.createRecord
    sample.int(1)
    val firstId = sample._id.get
    SampleSchema.save(sample) 

    sample.clearForReuse

    try {
      sample.int.getOpt
      fail("the record should be in a locked state")
    } catch {
      case e: LockedRecordException =>
      case e => fail("expected LockedRecordException, got " + e)
    }

    // reinit as a new record 
    sample.init(new BasicDBObject, true)
    val secondId = sample._id.get
    assertFalse(firstId =? secondId)
    assertEquals(None, sample.int.getOpt)
  }

  @Test
  def testFreelist: Unit = {
    val first = FreelistedRecSchema.createRecord
    FreelistedRecSchema.releaseRecord(first)
    val second = FreelistedRecSchema.createRecord
    
    assertSame(first, second)
  }
}
