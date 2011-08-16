package com.foursquare.meadow

import com.foursquare.meadow.Implicits._

/**
 * A base trait for any extensions to the behavior of a ValueContainer whose
 * underlying value type is T.
 */
trait Extension[T] {
  /**
   * Hook for notifications of changes to the value of the underlying
   * ValueContainer. Not invoked if 'set' is called but with the value it
   * already had. 
   */
  def onChange(oldVal: Option[T], newVal: Option[T]): Unit = ()
}
case class NoExtensions[T]() extends Extension[T]

/**
 * A mixin for extended fields that model a foreign key to a separate
 * collection.
 *
 * If the field is a foreign key and does not need additional logic, it's best
 * to use FKExtension defined below.
 */
trait ForeignKeyLogic[R <: Record[IdType], IdType] {
  self: Extension[IdType] =>

  protected def vc: ValueContainer[IdType, _, FKExtension[R, IdType]]
  protected def descriptor: Schema[R, IdType]

  private var _primedObj: Option[R] = None
  private var _isPrimed: Boolean = false

  override def onChange(old: Option[IdType], newVal: Option[IdType]) = {
    _primedObj = None
    _isPrimed = false
  }

  /**
   * Type-safe setter that calls the underlying ValueContainer with the
   * record's id and automatically primes the cached value.
   */
  def set(obj: R) = {
    vc.set(obj.id)
    primeObj(obj)
  }

  /**
   * Prime methods for when the associated object is known.
   */
  def primeObj(obj: R): Unit = primeObj(Some(obj))
  def primeObj(obj: Option[R]): Unit = synchronized {
    if (vc.getOpt !=? obj.map(_.id)) {
      throw new RuntimeException("priming obj with id %s on field with id %s".format(obj.map(_.id), vc.getOpt))
    }
    _primedObj = obj
    _isPrimed = true
  }

  /**
   * Fetches the object associated with the underlying ID and caches it.
   *
   * Note: it is dangerous to call this from within a loop as each individual
   * call will make its own roundtrip to the database. It is better to fetch in
   * bulk, then call prime. PrimingLogic and the 'prime' method on
   * RecordDescriptor help simplify that procedure. 
   */
  def fetchObj: Option[R] = synchronized {
    if (!_isPrimed) {
      _primedObj = vc.getOpt.flatMap(id => descriptor.findOne(id))
      _isPrimed = true
    }
    _primedObj
  }

  /**
   * Returns a previously primed or fetched object. If the object has not been
   * primed or fetched, throws an exception.
   */
  def primedObj: Option[R] = synchronized {
    if (!_isPrimed && vc.isDefined) {
      throw new RuntimeException("cannot call primedObj on an FK that has not been primed or had fetchObj invoked")
    }
    _primedObj
  }

  /**
   * Returns true iff primeObj, fetchObj, or the type-safe setter have been
   * invoked.
   */
  def isPrimed: Boolean = _isPrimed
}

class FKExtension[R <: Record[IdType], IdType](override val vc: ValueContainer[IdType, MaybeExists, FKExtension[R, IdType]],
                                               override val descriptor: Schema[R, IdType])
  extends Extension[IdType] with ForeignKeyLogic[R, IdType]

object PrimingLogic {
  /**
   * Looks up a batch of records referenced by foreign keys, and primes their
   * foreign key objects accordingly. Expects the descriptor associated with
   * the referenced records, the list of records that contain the foreign keys,
   * and a method to extract the foreign key from a record. 
   * 
   * If the caller already has a list of known records of the referenced type,
   * it can also pass that in to avoid possible lookups.
   */
  def prime[ContainingRecord <: Record[_],
            ReferencedRecord <: Record[IdType],
            IdType,
            Ext <: Extension[IdType] with ForeignKeyLogic[ReferencedRecord, IdType]](
      referencedDescriptor: Schema[ReferencedRecord, IdType],
      containingRecords: List[ContainingRecord],
      lambda: ContainingRecord => ValueContainer[IdType, MaybeExists, Ext],
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
