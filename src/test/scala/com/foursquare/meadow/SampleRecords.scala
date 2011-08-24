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

object ReferencedRecordSchema extends Schema[ReferencedRecord, ObjectId] {
  override protected def createInstance = new ReferencedRecord
  override protected def mongoLocation = MongoLocation("meadow-test", "ref")
}

class ReferencedRecord extends Record[ObjectId] {
  override def id = _id.get
  override def schema = ReferencedRecordSchema

  val _id = build("_id", name => objectIdField(name).required_!().withGenerator(ObjectIdGenerator))
  val name = build("name", name => stringField(name))
}


class Sample extends Record[ObjectId] {
  override def id = _id.get
  override def schema = SampleSchema

  val _id = build("_id", name => objectIdField(name).required_!().withGenerator(ObjectIdGenerator))
  val int = build("int", name => intField(name))
  val long = build("long", name => longField(name))
  val string = build("string", name => stringField(name))
  val double = build("double", name => doubleField(name))
  val embedded = build("embedded", name => recordField(name, SampleSchema))
  val enum = build("enum", name => FieldDescriptor[TestEnum.Value](name, MappedSerializer(TestEnum.values.toList.map(v => (v.toString, v)).toMap, _.toString)))
  val custom = build("custom", name => stringField(name).withExtensions[CustomExtension](vc => new CustomExtension(vc)))
  val refId = build("refId", name => objectIdField(name).withFKExtensions(ReferencedRecordSchema))
}

object SampleSchema extends Schema[Sample, ObjectId] {
  override protected def createInstance = new Sample
  override protected def mongoLocation = MongoLocation("meadow-test", "sample")
}


object FreelistedRecSchema extends Schema[FreelistedRec, ObjectId] {
  override protected def createInstance = new FreelistedRec
  override protected def mongoLocation = MongoLocation("meadow-test", "freelisted")
  override protected val allocator = new FreelistAllocator[FreelistedRec, ObjectId](() => this.createInstance)
}

class FreelistedRec extends Record[ObjectId] {
  override def id = _id.get
  override def schema = FreelistedRecSchema

  val _id = build("_id", name => objectIdField(name).required_!().withGenerator(ObjectIdGenerator))
  val int = build("int", name => intField(name))
  val long = build("long", name => longField(name))
}
