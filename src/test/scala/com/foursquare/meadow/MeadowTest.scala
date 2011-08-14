package com.foursquare.meadow

import org.bson.BSONObject
import com.mongodb.BasicDBObject
import net.liftweb.common.Loggable
import org.bson.types.ObjectId
import org.junit.{Before, Test}
import org.junit.Assert._

class LegacyFKExtension[R <: Record](vc: ExtendableValueContainer[Long, LegacyFKExtension[R]],
                                     desc: RecordDescriptor[R]) extends Extensions[Long] {
}

class FKExtension[R <: Record](vc: ExtendableValueContainer[ObjectId, FKExtension[R]],
                               desc: RecordDescriptor[R]) extends Extensions[ObjectId] {
}

class Venue protected(dbo: BSONObject, newRecord: Boolean) extends Record(dbo, newRecord) {
  override val descriptor = VenueDescriptor
}

object VenueDescriptor extends RecordDescriptor[Venue] {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new Venue(dbo, newRecord)

  val _id = objectIdField("_id").required().withGenerator(ObjectIdGenerator)
  val name = stringField("name").required()
}

object CheckinDescriptor extends RecordDescriptor[Checkin] {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new Checkin(dbo, newRecord)

  val _id = objectIdField("_id").required().withGenerator(ObjectIdGenerator)
  val userid = longField("uid").withExtensions[LegacyFKExtension[User]](vc => new LegacyFKExtension(vc, UserDescriptor))
  val legid = longField("legid")
  val venueid = objectIdField("venueid").withExtensions[FKExtension[Venue]](vc => new FKExtension(vc, VenueDescriptor))
  val eventid = objectIdField("eventid")
  val oauthconsumer = objectIdField("oa")
  val points = intField("points")
  val place = stringField("place")
  val shout = stringField("shout")

  val customLogicField = stringField("custom").withExtensions[CustomExtension](vc => new CustomExtension(vc))
}

class Checkin protected(dbo: BSONObject, newRecord: Boolean) extends Record(dbo, newRecord) {
  override val descriptor = CheckinDescriptor

  val _id           = build(descriptor._id)
  val userid        = build(descriptor._id)
  val legid         = build(descriptor.legid)
  val venueid       = build(descriptor.venueid)
  val eventid       = build(descriptor.eventid)
  val oauthconsumer = build(descriptor.oauthconsumer)
  val points        = build(descriptor.points)
  val place         = build(descriptor.place)
  val shout         = build(descriptor.shout)
  val customLogicField = build(descriptor.customLogicField)
}

class User protected(dbo: BSONObject, newRecord: Boolean) extends Record(dbo, newRecord) {
  override val descriptor = UserDescriptor

  val lastCheckin = build(descriptor.lastCheckin)
}

object UserDescriptor extends RecordDescriptor[User] {
  override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new User(dbo, newRecord)

  val lastCheckin = recordField("c_obj", CheckinDescriptor)
}

class CustomExtension(vc: ExtendableValueContainer[String, CustomExtension]) extends Extensions[String] {
  def someCustomMethod = "whee " + vc.getOpt
}

class MeadowTest {
  @Before
  def setUp: Unit = {
    // connect to mongo
  }

  @Test
  def instantiateCheckin: Unit = {
    val checkin = CheckinDescriptor.loadRecord(new BasicDBObject("_id", new ObjectId())) 
    println(checkin)
    println(checkin._id.get)
    checkin.customLogicField.ext.someCustomMethod
    val checkinSerializer = new RecordSerializer(CheckinDescriptor)
    println(checkinSerializer.serialize(checkin))
  }

  @Test
  def customField: Unit = {
    object CustomizedRecDescriptor extends RecordDescriptor[CustomizedRec] {
      override protected def createInstance(dbo: BSONObject, newRecord: Boolean) = new CustomizedRec(dbo, newRecord)

      val customLogicField = stringField("custom").withExtensions[CustomExtension](vc => new CustomExtension(vc))
    }

    class CustomizedRec protected(dbo: BSONObject, newRecord: Boolean) extends Record(dbo, newRecord) {
      override val descriptor = CustomizedRecDescriptor
      val customLogicField = build(descriptor.customLogicField)
    }

    val rec = CustomizedRecDescriptor.createRecord
    assertEquals(rec.customLogicField.ext.someCustomMethod, "whee None")
    rec.customLogicField.set("foo")
    assertEquals(rec.customLogicField.ext.someCustomMethod, "whee Some(foo)")
  }

  @Test
  def setCheckinOnUser: Unit = {
    val user = UserDescriptor.createRecord
    val checkin = CheckinDescriptor.createRecord
    user.lastCheckin.set(checkin)
    println(checkin._id.get)

    println(user)
    println(user.lastCheckin.getOpt)
    println(user.lastCheckin.getOpt.map(_._id.get))

    val checkinSerializer = new RecordSerializer(CheckinDescriptor)
    println(checkinSerializer.serialize(checkin))

    val userSerializer = new RecordSerializer(UserDescriptor)
    println(userSerializer.serialize(user))
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
}
