package com.foursquare.meadow

import com.mongodb.BasicDBObject
import java.util.Date
import org.bson.types.{BasicBSONList, ObjectId}
import org.joda.time.DateTime
import org.junit.Assert._
import org.junit.Test

class SerializerTest {
  val oidS = ObjectIdSerializer
  val intS = IntSerializer
  val longS = LongSerializer
  val doubleS = DoubleSerializer
  val stringS = StringSerializer
  val dateTimeS = DateTimeSerializer
  val booleanS = BooleanSerializer
  
  val Epsilon: Double = .0001

  @Test
  def testObjectId: Unit = {
    val oid = new ObjectId()
    assertEquals(ObjectIdWrapper(oid), oidS.serialize(oid))
    assertEquals(oid, oidS.deserialize(oidS.serialize(oid)))
    assertEquals(oid, oidS.deserialize(StringWrapper(oid.toString)))
  }

  @Test(expected = classOf[RuntimeException])
  def testOIDThrows1: Unit = { oidS.deserialize(IntWrapper(1)) }

  @Test(expected = classOf[RuntimeException])
  def testOIDThrows2: Unit = { oidS.deserialize(StringWrapper("not a valid oid")) }

  @Test
  def testInt: Unit = {
    val int = 3
    assertEquals(IntWrapper(int), intS.serialize(int))
    assertEquals(int, intS.deserialize(intS.serialize(int)))
  }

  @Test(expected = classOf[RuntimeException])
  def testIntThrows1: Unit = { intS.deserialize(StringWrapper("oijoijoi")) }

  @Test
  def testLong: Unit = {
    val long = 3L
    assertEquals(LongWrapper(long), longS.serialize(long))
    assertEquals(long, longS.deserialize(longS.serialize(long)))
    assertEquals(4, longS.deserialize(IntWrapper(4)))
  }

  @Test(expected = classOf[RuntimeException])
  def testLongThrows1: Unit = { longS.deserialize(StringWrapper("fdsaf")) }

  @Test
  def testString: Unit = {
    val string = "whee"
    assertEquals(StringWrapper(string), stringS.serialize(string))
    assertEquals(string, stringS.deserialize(stringS.serialize(string)))
  }

  @Test(expected = classOf[RuntimeException])
  def testStringThrows1: Unit = { stringS.deserialize(IntWrapper(123)) }

  @Test
  def testDouble: Unit = {
    val double = 2.3
    assertEquals(double, doubleS.deserialize(doubleS.serialize(double)), Epsilon)
    assertEquals(2.0, doubleS.deserialize(IntWrapper(2)), Epsilon)
    assertEquals(2.0, doubleS.deserialize(LongWrapper(2)), Epsilon)
  }

  @Test(expected = classOf[RuntimeException])
  def testDoubleThrows1: Unit = { doubleS.deserialize(StringWrapper("foo")) }

  @Test
  def testDateTime: Unit = {
    assertEquals(new DateTime(1234), dateTimeS.deserialize(DateWrapper(new Date(1234))))
  }

  @Test(expected = classOf[RuntimeException])
  def testDateTimeThrows1: Unit = { dateTimeS.deserialize(StringWrapper("foo")) }

  @Test
  def testBoolean: Unit = {
    assertTrue(booleanS.deserialize(booleanS.serialize(true)))
    assertFalse(booleanS.deserialize(booleanS.serialize(false)))
  }
  
  @Test(expected = classOf[RuntimeException])
  def testBooleanThrows1: Unit = { booleanS.deserialize(StringWrapper("foo")) }

  @Test
  def testListOfInts: Unit = {
    val ls = ListSerializer(IntSerializer)
    assertEquals(List(2,3), ls.deserialize(ls.serialize(List(2, 3))))
    assertEquals(List(), ls.deserialize(ls.serialize(List())))
  }

  @Test
  def testLatLongLike: Unit = {
    val ls = ListSerializer(DoubleSerializer)
    assertEquals(List(40.73, -73.98), ls.deserialize(ls.serialize(List(40.73, -73.98))))
  }

  @Test
  def testListOfListOfInts: Unit = {
    val ls = ListSerializer(ListSerializer(IntSerializer))
    assertEquals(List(List(2), List(3)), ls.deserialize(ls.serialize(List(List(2), List(3)))))
    assertEquals(List(), ls.deserialize(ls.serialize(List())))
  }

  @Test
  def testEnum: Unit = {
    val enumS = MappedSerializer[TestEnum.Value](TestEnum.values.toList.map(v => (v.toString, v)).toMap, _.toString)
    assertEquals(TestEnum.One, enumS.deserialize(enumS.serialize(TestEnum.One)))
    assertEquals(TestEnum.Two, enumS.deserialize(enumS.serialize(TestEnum.Two)))
    assertEquals(TestEnum.Two, enumS.deserialize(StringWrapper("two")))
  }

  @Test(expected = classOf[RuntimeException])
  def testEnumThrows: Unit = {
    val enumS = MappedSerializer[TestEnum.Value](TestEnum.values.toList.map(v => (v.toString, v)).toMap, _.toString)
    enumS.deserialize(IntWrapper(1))
  }

  @Test
  def testRecord: Unit = {
    val sampleS = RecordSerializer(SampleDescriptor)
    val sample = SampleDescriptor.createRecord
    sample.int.set(1)
    sample.long.set(2L)
    sample.string.set("outer")
    sample.double.set(3.0)
    sample.enum.set(TestEnum.One)
    val innerSample = SampleDescriptor.createRecord
    innerSample.string.set("inner")
    sample.embedded.set(innerSample)

    val innerMap = new java.util.HashMap[String, Any]()
    innerMap.put("string", "inner")
    innerMap.put("_id", innerSample._id.get)
    val outerMap = new java.util.HashMap[String, Any]()
    outerMap.put("_id", sample._id.get)
    outerMap.put("int", 1)
    outerMap.put("long", 2L)
    outerMap.put("string", "outer")
    outerMap.put("double", 3.0)
    outerMap.put("embedded", new BasicDBObject(innerMap))
    outerMap.put("enum", "one")
    assertEquals(BSONObjectWrapper(new BasicDBObject(innerMap)), sampleS.serialize(innerSample))
    assertEquals(BSONObjectWrapper(new BasicDBObject(outerMap)), sampleS.serialize(sample))
  }
}
