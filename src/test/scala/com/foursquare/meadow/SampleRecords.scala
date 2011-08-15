package com.foursquare.meadow

import org.bson.BSONObject
import org.bson.types.ObjectId
  
object TestEnum extends Enumeration {
  val One = Value("one")
  val Two = Value("two")
}

class CustomExtension(vc: ExtendableValueContainer[String, CustomExtension]) extends Extension[String] {
  def someCustomMethod = "whee " + vc.getOpt
}

object ReferencedRecordSchema extends Schema[ReferencedRecord, ObjectId] {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new ReferencedRecord(dbo, newRecord)
  override protected def mongoLocation = MongoLocation("test", "ref")

  val _id = objectIdField("_id").required_!().withGenerator(ObjectIdGenerator)
  val name = stringField("name")

  trait FK {
    val refId = objectIdField("refId").withFKExtensions(ReferencedRecordSchema)
  }

  class FKList[R <: Record[_] with TestThing, Ext <: Extension[ObjectId] with ForeignKeyLogic[ReferencedRecord, ObjectId]](lst: List[R],
                                           lambda: R => ExtendableValueContainer[ObjectId, Ext]) {
    def primeRefs = ReferencedRecordSchema.prime(lst, lambda)
  }
}

class ReferencedRecord protected(dbo: BSONObject, newRecord: Boolean) extends Record[ObjectId](dbo, newRecord) {
  override val schema = ReferencedRecordSchema
  override def id = _id.get

  val _id = build(schema._id)
  val name = build(schema.name)
}


object SampleSchema extends Schema[Sample, ObjectId]
                    with ReferencedRecordSchema.FK {
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

trait TestThing {
  val refId: ExtendableValueContainer[ObjectId, Extension[ObjectId] with ForeignKeyLogic[ReferencedRecord, ObjectId]]
}

class Sample protected(dbo: BSONObject, newRecord: Boolean)
    extends Record[ObjectId](dbo, newRecord)
    with TestThing {
  override val schema = SampleSchema
  override def id = _id.get

  val _id = build(schema._id)
  val int = build(schema.int)
  val long = build(schema.long)
  val string = build(schema.string)
  val double = build(schema.double)
  val embedded = build(schema.embedded)
  val enum = build(schema.enum)
  val custom = build(schema.custom)

  // hrm, this sucks. should probably be another trait somehow?
  val refId = build(schema.refId) 
}

object PrimingImplicits {
  implicit def refRecFKsToPrimable[R <: Record[_] with TestThing](lst: List[R]): ReferencedRecordSchema.FKList[R, _] = new ReferencedRecordSchema.FKList(lst, (r: R) => r.refId)
}
