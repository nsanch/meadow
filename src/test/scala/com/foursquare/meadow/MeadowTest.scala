package com.foursquare.meadow

import org.bson.BSONObject
import com.mongodb.BasicDBObject
import net.liftweb.common.Loggable
import org.bson.types.ObjectId
import org.junit.{Before, Test}
import org.junit.Assert._


class MeadowTest {
  @Test
  def customField: Unit = {
    val rec = SampleSchema.createRecord
    assertEquals(rec.custom.ext.someCustomMethod, "whee None")
    rec.custom("foo")
    assertEquals(rec.custom.ext.someCustomMethod, "whee Some(foo)")
  }

  @Test
  def setEmbeddedRecord: Unit = {
    val outerSample = SampleSchema.createRecord
    val innerSample = SampleSchema.createRecord

    outerSample.embedded(innerSample)

    val outerId = outerSample.id
    val innerId = innerSample.id

    SampleSchema.save(outerSample)

    val outerSample2 = SampleSchema.findOne(outerId).get
    assertEquals(outerSample2.embedded.getOpt.get.id, innerId)
  }
  
  @Test
  def testSettingEnum: Unit = {
    val rec = SampleSchema.createRecord
    assertEquals(rec.enum.getOpt, None)

    rec.enum(TestEnum.Two)
    assertEquals(rec.enum.getOpt, Some(TestEnum.Two))
    
    rec.enum(None)
    assertEquals(rec.enum.getOpt, None)
  }

  @Test
  def testFK: Unit = {
    val ref = ReferencedRecordSchema.createRecord
    val refId = ref.id

    val sample = SampleSchema.createRecord
    val sampleId = sample.id
    sample.refId.ext(ref)
    assertEquals(sample.refId.getOpt, Some(refId))

    SampleSchema.save(sample)
    ReferencedRecordSchema.save(ref)
    
    val sample2 = SampleSchema.findOne(sampleId).get
    assertEquals(sample2.refId.ext.fetchObj.get.id, refId)
    assertEquals(sample2.refId.ext.primedObj.get.id, refId)
  }

  @Test
  def testFKPrimedManually: Unit = {
    val ref = ReferencedRecordSchema.createRecord
    val refId = ref.id

    val sample = SampleSchema.createRecord
    sample.refId.ext(ref)
    assertEquals(sample.refId.getOpt, Some(refId))

    sample.refId.ext.primeObj(ref)
    assertEquals(sample.refId.ext.fetchObj.get.id, refId)
    assertEquals(sample.refId.ext.primedObj.get.id, refId)
  }

  @Test(expected = classOf[RuntimeException])
  def testUnprimedFKThrows: Unit = {
    val ref = ReferencedRecordSchema.createRecord
    val refId = ref.id

    val sample = SampleSchema.createRecord
    // This uses the ValueContainer's set() instead of the FKExtension's set()
    // because the latter set() does priming implicitly.
    sample.refId(ref.id)
    assertEquals(sample.refId.getOpt, Some(refId))

    ReferencedRecordSchema.save(ref)
    // This should throw even through the object exists.
    sample.refId.ext.primedObj.get
  }

  @Test
  def testPriming: Unit = {
    val (ref1, ref2, ref3, ref4) = 
        (ReferencedRecordSchema.createRecord,
         ReferencedRecordSchema.createRecord,
         ReferencedRecordSchema.createRecord,
         ReferencedRecordSchema.createRecord)

    ref1.name("foo")
    ref2.name("bar")
    ref3.name("baz")
    ref4.name("boo")
    val (refId1, refId2) = (ref1.id, ref2.id)
    ReferencedRecordSchema.save(ref1)
    ReferencedRecordSchema.save(ref2)
    // don't save ref3 or ref4 to be sure that a fetch for them would fail.

    val (sample1, sample2, sample3, sample4, sample5, sample6) = 
        (SampleSchema.createRecord,
         SampleSchema.createRecord,
         SampleSchema.createRecord,
         SampleSchema.createRecord,
         SampleSchema.createRecord,
         SampleSchema.createRecord)

    sample1.refId(refId1)
    sample2.refId(refId1)
    sample3.refId(refId2)
    // sample4 left empty
    // sample5 is primed ahead of time with ref3, which doesn't exist in the db.
    sample5.refId.ext(ref3)
    // sample6 points at ref4, which wasn't persisted, so it can only come from
    // the list of known records we pass to prime.
    sample6.refId(ref4.id)

    ReferencedRecordSchema.prime(List(sample1, sample2, sample3, sample4, sample5),
                                     (s: Sample) => s.refId,
                                     known = List(ref4))
  }
}
