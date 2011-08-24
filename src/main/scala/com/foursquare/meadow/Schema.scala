package com.foursquare.meadow

import org.bson.BSONObject
import com.mongodb.{BasicDBObject, DBCollection, DBObject, WriteConcern, BasicDBList}
import org.bson.types.ObjectId
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer

case class MongoLocation(db: String, collection: String)

/**
 * A simple base class that doesn't need any of the type parameters that Schema
 * does. Provides helper methods for schema construction and some basic methods
 * for interacting with Mongo.
 */
abstract class BaseSchema {
  // Specifies the location of this collection in mongo.
  protected def mongoLocation: MongoLocation

  /**
   * Returns the mongo DBCollection associated with this Schema.
   */
  // TODO(nsanch): As much as possible, the mongo logic should move to another class.
  def coll(): DBCollection = MongoConnector.mongo.getDB(mongoLocation.db).getCollection(mongoLocation.collection)
 
  /**
   * Saves the given record to mongo. 
   */
  def save(r: Record[_]) = {
    // TODO(nsanch): is it better to do insert for new records? appears to work
    // fine either way.
    coll().save(r.serialize(), WriteConcern.NORMAL)
  }

  private var fieldDescriptors: Map[String, BaseFieldDescriptor] = Map()
  def getOrCreateFD[T, Reqd <: MaybeExists, Ext <: Extension[T]](name: String, fdCreator: String => FieldDescriptor[T, Reqd, Ext]): FieldDescriptor[T, Reqd, Ext] = {
    fieldDescriptors.get(name).map(_.asInstanceOf[FieldDescriptor[T, Reqd, Ext]]).getOrElse({
      val fd = fdCreator(name)
      fieldDescriptors += (name -> fd)
      fd
    })
  }

}

/**
 * Models a collection in Mongo whose primary key is of the given IdType.
 * 
 * Provides simple methods for creating, finding, and saving records to the
 * modeled collection. More complicated queries and updates aren't supported,
 * and should instead be issued via a separate library like Rogue.
 */
abstract class Schema[RecordType <: Record[IdType], IdType] extends BaseSchema {
  protected def createInstance: RecordType
  protected def allocator: RecordAllocator[RecordType] = _allocator
  private lazy val _allocator = new NormalRecordAllocator[RecordType, IdType](() => this.createInstance)

  def createRecord: RecordType = {
    val inst = allocator.construct 
    inst.init(new BasicDBObject(), true)
    inst
  }

  def releaseRecord(rec: RecordType) = {
    allocator.destroy(rec)
  }

  private[meadow] def loadRecord(dbo: BSONObject): RecordType = {
    val inst = allocator.construct 
    inst.init(dbo, false)
    inst
  }
  
  /**
   * Finds one or many instances of RecordType by ID. More complicated queries
   * aren't supported, and should instead be issued via a separate library like
   * Rogue.
   *
   * Note: you should take care to pass in a reasonably-sized list to avoid
   * passing too large of a query to Mongo.
   */
  def findOne(id: IdType): Option[RecordType] = findAll(List(id)).headOption
  def findAll(ids: Traversable[IdType]): List[RecordType] = {
    if (ids.isEmpty) {
      Nil
    } else {
      val idList = new BasicDBList()
      ids.foreach(id => idList.add(id.asInstanceOf[AnyRef]))
      val found = coll().find(new BasicDBObject("_id", new BasicDBObject("$in", idList))) 
      val l = new ListBuffer[RecordType]()
      l.sizeHint(ids)
      while (found.hasNext()) {
        l += loadRecord(found.next())
      }
      l.result()
    }
  }

  /**
   * A simple wrapper around PrimingLogic.prime that simplifies priming.
   * Accepts a list of records that have foreign keys to RecordType, and
   * fetches those referenced records in a batch.
   */
  def prime[ContainingRecord <: Record[_],
            Ext <: Extension[IdType] with ForeignKeyLogic[RecordType, IdType]](
      containingRecords: List[ContainingRecord],
      lambda: ContainingRecord => ValueContainer[IdType, ContainingRecord, MaybeExists, Ext],
      known: List[RecordType] = Nil): List[ContainingRecord] = {
    PrimingLogic.prime(this, containingRecords, lambda, known)
  }
}
