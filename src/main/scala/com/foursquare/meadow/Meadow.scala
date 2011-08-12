package com.foursquare.meadow

import com.mongodb.{BasicDBObject, DBObject}
import net.liftweb.common.Loggable
import org.bson.types.ObjectId

/*
sealed abstract class DBObjectSerializer[T] extends Loggable {
  def write(name: String, value: Option[T], dest: DBObject): Unit = {
    value.map(v => dest.put(name, v))
  }

}
case object IntSerializer extends DBObjectSerializer[Int] {
  def parseFromAny(value: Any): Option[T] = value match {
    case i: Int => Some(i)
    case _ => None
  }
}

abstract class FieldWithDefault[T](name: String, val defaultValue: T) extends Field[T](name) {
  override def getOpt: Option[T] = super.getOpt.orElse(Some(defaultValue))
  def get: T = getOpt.getOrElse(defaultValue)
}
*/


object CheckinDescriptor extends RecordDescriptor {
  def create(dbo: DBObject) = new Checkin(dbo)

  val _id = new ObjectIdFieldDescriptor("_id")
  val userid = new LongFieldDescriptor("uid")
  val legid = new LongFieldDescriptor("legid")
  val venueid = new ObjectIdFieldDescriptor("venueid")
  val eventid = new ObjectIdFieldDescriptor("eventid")
  val oauthconsumer = new ObjectIdFieldDescriptor("oa")
  val points = new IntFieldDescriptor("points")
  val place = new StringFieldDescriptor("place")
  val shout = new StringFieldDescriptor("shout")
  
  val fields = List(_id,
                    userid,
                    legid,
                    venueid,
                    eventid,
                    oauthconsumer,
                    points,
                    place,
                    shout)
}

class Checkin(val dbo: DBObject) extends Record(dbo) {
  override def descriptor = CheckinDescriptor
}
