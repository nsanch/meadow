package com.foursquare.meadow

import org.bson.BSONObject
  
object TestEnum extends Enumeration {
  val One = Value("one")
  val Two = Value("two")
}


object SampleDescriptor extends RecordDescriptor[Sample] {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new Sample(dbo, newRecord)
  override protected def mongoLocation = MongoLocation("test", "sample")

  val _id = objectIdField("_id").required().withGenerator(ObjectIdGenerator)
  val int = intField("int")
  val long = longField("long")
  val string = stringField("string")
  val double = doubleField("double")
  val embedded = recordField("embedded", this)
  val enum = FieldDescriptor[TestEnum.Value]("enum", MappedSerializer(TestEnum.values.toList.map(v => (v.toString, v)).toMap, _.toString))
}

class Sample protected(dbo: BSONObject, newRecord: Boolean) extends Record(dbo, newRecord) {
  override val descriptor = SampleDescriptor

  val _id = build(descriptor._id)
  val int = build(descriptor.int)
  val long = build(descriptor.long)
  val string = build(descriptor.string)
  val double = build(descriptor.double)
  val embedded = build(descriptor.embedded)
  val enum = build(descriptor.enum)
}
