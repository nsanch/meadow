package com.foursquare.meadow

import org.bson.types.ObjectId

abstract class Generator[T] {
  def generate(): T
}

case object ObjectIdGenerator extends Generator[ObjectId] {
  def generate() = new ObjectId()
}
