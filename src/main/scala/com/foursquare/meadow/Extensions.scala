package com.foursquare.meadow

import com.foursquare.meadow.Implicits._

trait Extensions[T] {
  def onChange(oldVal: Option[T], newVal: Option[T]): Unit = ()
}
case class NoExtensions[T]() extends Extensions[T]

/**
 * A mixin for extended fields that model a foreign key to a separate
 * collection.
 *
 * If the field is a foreign key and does not need additional logic, it's best
 * to use FKExtension defined below.
 */
trait ForeignKeyLogic[R <: Record[IdType], IdType] {
  self: Extensions[IdType] =>

  protected def vc: ExtendableValueContainer[IdType, FKExtension[R, IdType]]
  protected def descriptor: RecordDescriptor[R, IdType]

  private var _primedObj: Option[R] = None
  private var _isPrimed: Boolean = false

  override def onChange(old: Option[IdType], newVal: Option[IdType]) = {
    _primedObj = None
    _isPrimed = false
  }

  def set(obj: R) = {
    vc.set(obj.id)
    primeObj(obj)
  }

  def primeObj(obj: R): Unit = primeObj(Some(obj))
  def primeObj(obj: Option[R]): Unit = synchronized {
    if (vc.getOpt !=? obj.map(_.id)) {
      throw new RuntimeException("priming obj with id %s on field with id %s".format(obj.map(_.id), vc.getOpt))
    }
    _primedObj = obj
    _isPrimed = true
  }

  def fetchObj: Option[R] = synchronized {
    if (!_isPrimed) {
      _primedObj = vc.getOpt.flatMap(id => descriptor.findOne(id))
      _isPrimed = true
    }
    _primedObj
  }

  def primedObj: Option[R] = synchronized {
    if (!_isPrimed && vc.isDefined) {
      throw new RuntimeException("cannot call primedObj on an FK that has not been primed or had fetchObj invoked")
    }
    _primedObj
  }

  def isPrimed: Boolean = _isPrimed
}

class FKExtension[R <: Record[IdType], IdType](override val vc: ExtendableValueContainer[IdType, FKExtension[R, IdType]],
                                               override val descriptor: RecordDescriptor[R, IdType])
  extends Extensions[IdType] with ForeignKeyLogic[R, IdType]

object PrimingLogic {
  def prime[ContainingRecord <: Record[_],
            ReferencedRecord <: Record[IdType],
            IdType,
            Ext <: Extensions[IdType] with ForeignKeyLogic[ReferencedRecord, IdType]](
      referencedDescriptor: RecordDescriptor[ReferencedRecord, IdType],
      containingRecords: List[ContainingRecord],
      lambda: ContainingRecord => ExtendableValueContainer[IdType, Ext],
      known: List[ReferencedRecord] = Nil): List[ContainingRecord] = {
 
    val (primedRecords, unprimedRecords) = containingRecords.partition(cr => !lambda(cr).isDefined || lambda(cr).ext.isPrimed)
    val knownMap: Map[IdType, ReferencedRecord] = {
      val passedIn = known.map(r => (r.id, r)).toMap
      val fromGivenRecords = (
        (for (pr <- primedRecords;
             vc = lambda(pr);
             referencedId <- vc.getOpt;
             referencedRecord <- vc.ext.primedObj if vc.ext.isPrimed) yield {
          (referencedId, referencedRecord)
        }).toMap
      )
      passedIn ++ fromGivenRecords
    }

    val allIdsToPrime: Map[Option[IdType], List[ContainingRecord]] = unprimedRecords.groupBy(cr => lambda(cr).getOpt).toMap
    val (idsWeKnow, idsToLookup) = unprimedRecords.flatMap(cr => lambda(cr).getOpt).partition(knownMap.contains _)

    val foundMap: Map[IdType, ReferencedRecord] = referencedDescriptor.findAll(idsToLookup).map(r => (r.id, r)).toMap

    val allKnownIncludingFound = knownMap ++ foundMap
    for (id <- idsToLookup;
         associatedContainingRecords <- allIdsToPrime.get(Some(id));
         oneContainingRecord <- associatedContainingRecords) {
      lambda(oneContainingRecord).ext.primeObj(allKnownIncludingFound.get(id))
    }

    containingRecords
  }
}
