package com.foursquare.meadow

import org.bson.types.ObjectId

/**
 * Generates a new value of T, often an ID for a newly-created record.
 */
abstract class Generator[T] {
  def generate(): T
}

case object ObjectIdGenerator extends Generator[ObjectId] {
  def generate() = new ObjectId()
}
