package com.foursquare.meadow

import org.bson.BSONObject
import org.bson.types.ObjectId
  
object TestEnum extends Enumeration {
  val One = Value("one")
  val Two = Value("two")
}

class CustomExtension(vc: ExtendableValueContainer[String, CustomExtension]) extends Extensions[String] {
  def someCustomMethod = "whee " + vc.getOpt
}

object ReferencedRecordDescriptor extends RecordDescriptor[ReferencedRecord, ObjectId] {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new ReferencedRecord(dbo, newRecord)
  override protected def mongoLocation = MongoLocation("test", "ref")

  val _id = objectIdField("_id").required_!().withGenerator(ObjectIdGenerator)
  val name = stringField("name")

  trait FK {
    val refId = objectIdField("refId").withExtensions[FKExtension[ReferencedRecord, ObjectId]](vc => new FKExtension(vc, ReferencedRecordDescriptor))
  }
}

class ReferencedRecord protected(dbo: BSONObject, newRecord: Boolean) extends Record[ObjectId](dbo, newRecord) {
  override val descriptor = ReferencedRecordDescriptor
  override def id = _id.get

  val _id = build(descriptor._id)
  val name = build(descriptor.name)
}


object SampleDescriptor extends RecordDescriptor[Sample, ObjectId]
                        with ReferencedRecordDescriptor.FK {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new Sample(dbo, newRecord)
  override protected def mongoLocation = MongoLocation("test", "sample")

  val _id = objectIdField("_id").required_!().withGenerator(ObjectIdGenerator)
  val int = intField("int")
  val long = longField("long")
  val string = stringField("string")
  val double = doubleField("double")
  val embedded = recordField("embedded", this)
  val enum = FieldDescriptor[TestEnum.Value]("enum", MappedSerializer(TestEnum.values.toList.map(v => (v.toString, v)).toMap, _.toString))
  val custom = stringField("custom").withExtensions[CustomExtension](vc => new CustomExtension(vc))
}

class Sample protected(dbo: BSONObject, newRecord: Boolean) extends Record[ObjectId](dbo, newRecord) {
  override val descriptor = SampleDescriptor
  override def id = _id.get

  val _id = build(descriptor._id)
  val int = build(descriptor.int)
  val long = build(descriptor.long)
  val string = build(descriptor.string)
  val double = build(descriptor.double)
  val embedded = build(descriptor.embedded)
  val enum = build(descriptor.enum)
  val custom = build(descriptor.custom)
  // hrm, this sucks. should probably be another trait somehow?
  val refId = build(descriptor.refId) 
}
