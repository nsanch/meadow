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
    assertEquals(sample2.refId.ext.cachedObj.get.id, refId)
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
    assertEquals(sample.refId.ext.cachedObj.get.id, refId)
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
    sample.refId.ext.cachedObj.get
  }
}
