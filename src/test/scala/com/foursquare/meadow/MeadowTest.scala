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
    val rec = SampleDescriptor.createRecord
    assertEquals(rec.custom.ext.someCustomMethod, "whee None")
    rec.custom.set("foo")
    assertEquals(rec.custom.ext.someCustomMethod, "whee Some(foo)")
  }

  @Test
  def setEmbeddedRecord: Unit = {
    val outerSample = SampleDescriptor.createRecord
    val innerSample = SampleDescriptor.createRecord

    outerSample.embedded.set(innerSample)

    val outerId = outerSample.id
    val innerId = innerSample.id

    outerSample.save

    val outerSample2 = SampleDescriptor.findOne(outerId).get
    assertEquals(outerSample2.embedded.getOpt.get.id, innerId)
  }
  
  @Test
  def testSettingEnum: Unit = {
    val rec = SampleDescriptor.createRecord
    assertEquals(rec.enum.getOpt, None)

    rec.enum.set(TestEnum.Two)
    assertEquals(rec.enum.getOpt, Some(TestEnum.Two))
    
    rec.enum.unset
    assertEquals(rec.enum.getOpt, None)
  }

  @Test
  def testFK: Unit = {
    val ref = ReferencedRecordDescriptor.createRecord
    val refId = ref.id

    val sample = SampleDescriptor.createRecord
    val sampleId = sample.id
    sample.refId.ext.set(ref)
    assertEquals(sample.refId.getOpt, Some(refId))

    sample.save
    ref.save
    
    val sample2 = SampleDescriptor.findOne(sampleId).get
    assertEquals(sample2.refId.ext.fetchObj.get.id, refId)
    assertEquals(sample2.refId.ext.primedObj.get.id, refId)
  }

  @Test
  def testFKPrimedManually: Unit = {
    val ref = ReferencedRecordDescriptor.createRecord
    val refId = ref.id

    val sample = SampleDescriptor.createRecord
    sample.refId.ext.set(ref)
    assertEquals(sample.refId.getOpt, Some(refId))

    sample.refId.ext.primeObj(ref)
    assertEquals(sample.refId.ext.fetchObj.get.id, refId)
    assertEquals(sample.refId.ext.primedObj.get.id, refId)
  }

  @Test(expected = classOf[RuntimeException])
  def testUnprimedFKThrows: Unit = {
    val ref = ReferencedRecordDescriptor.createRecord
    val refId = ref.id

    val sample = SampleDescriptor.createRecord
    // This uses the ValueContainer's set() instead of the FKExtension's set()
    // because the latter set() does priming implicitly.
    sample.refId.set(ref.id)
    assertEquals(sample.refId.getOpt, Some(refId))

    ref.save
    // This should throw even through the object exists.
    sample.refId.ext.primedObj.get
  }

  @Test
  def testPriming: Unit = {
    val (ref1, ref2, ref3, ref4) = 
        (ReferencedRecordDescriptor.createRecord,
         ReferencedRecordDescriptor.createRecord,
         ReferencedRecordDescriptor.createRecord,
         ReferencedRecordDescriptor.createRecord)

    ref1.name.set("foo")
    ref2.name.set("bar")
    ref3.name.set("baz")
    ref4.name.set("boo")
    val (refId1, refId2) = (ref1.id, ref2.id)
    ref1.save
    ref2.save
    // don't save ref3 or ref4 to be sure that a fetch for them would fail.

    val (sample1, sample2, sample3, sample4, sample5, sample6) = 
        (SampleDescriptor.createRecord,
         SampleDescriptor.createRecord,
         SampleDescriptor.createRecord,
         SampleDescriptor.createRecord,
         SampleDescriptor.createRecord,
         SampleDescriptor.createRecord)

    sample1.refId.set(refId1)
    sample2.refId.set(refId1)
    sample3.refId.set(refId2)
    // sample4 left empty
    // sample5 is primed ahead of time with ref3, which doesn't exist in the db.
    sample5.refId.ext.set(ref3)
    // sample6 points at ref4, which wasn't persisted, so it can only come from
    // the list of known records we pass to prime.
    sample6.refId.set(ref4.id)

    //ReferencedRecordDescriptor.prime(List(sample1, sample2, sample3, sample4, sample5),
    //                                 (s: Sample) => s.refId,
    //                                 known = List(ref4))
    import PrimingImplicits._
    List(sample1, sample2, sample3, sample4, sample5).primeRefs
  }
}
