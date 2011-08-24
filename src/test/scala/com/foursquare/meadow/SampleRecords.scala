package com.foursquare.meadow

import org.bson.BSONObject
import org.bson.types.ObjectId
  
object TestEnum extends Enumeration {
  val One = Value("one")
  val Two = Value("two")
}

class CustomExtension(vc: ValueContainer[String, _, CustomExtension]) extends Extension[String] {
  def someCustomMethod = "whee " + vc.getOpt
}

object ReferencedRecordSchema extends Schema[ReferencedRecord, ObjectId](new ReferencedRecord) {
  override protected def createInstance = new ReferencedRecord
  override protected def mongoLocation = MongoLocation("meadow-test", "ref")
}

class ReferencedRecord extends Record[ObjectId] {
  override def id = _id.get

  val _id = build(objectIdField("_id").required_!().withGenerator(ObjectIdGenerator))
  val name = build(stringField("name"))
}


class Sample extends Record[ObjectId] {
  override def id = _id.get

  val _id = build(objectIdField("_id").required_!().withGenerator(ObjectIdGenerator))
  val int = build(intField("int"))
  val long = build(longField("long"))
  val string = build(stringField("string"))
  val double = build(doubleField("double"))
  val embedded = build(recordField("embedded", SampleSchema))
  val enum = build(FieldDescriptor[TestEnum.Value]("enum", MappedSerializer(TestEnum.values.toList.map(v => (v.toString, v)).toMap, _.toString)))
  val custom = build(stringField("custom").withExtensions[CustomExtension](vc => new CustomExtension(vc)))
  val refId = build(objectIdField("refId").withFKExtensions(ReferencedRecordSchema))
}

object SampleSchema extends Schema[Sample, ObjectId](new Sample) {
  override protected def createInstance = new Sample
  override protected def mongoLocation = MongoLocation("meadow-test", "sample")
}


object FreelistedRecSchema extends Schema[FreelistedRec, ObjectId](new FreelistedRec) {
  override protected def createInstance = new FreelistedRec
  override protected def mongoLocation = MongoLocation("meadow-test", "freelisted")
  override protected val allocator = new FreelistAllocator[FreelistedRec, ObjectId](() => this.createInstance)
}

class FreelistedRec extends Record[ObjectId] {
  override def id = _id.get

  val _id = build(objectIdField("_id").required_!().withGenerator(ObjectIdGenerator))
  val int = build(intField("int"))
  val long = build(longField("long"))
}
