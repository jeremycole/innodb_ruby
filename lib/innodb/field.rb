# -*- encoding : utf-8 -*-
require "innodb/field_type"

# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records, including the length
# of the fixed-width and variable-width portion of the field.
class Innodb::Field
  attr_reader :position, :type

  def initialize(position, data_type, *properties)
    @position = position
    @type = Innodb::FieldType.new(data_type.to_s, properties)
  end

  # Return whether this field is NULL.
  def null?(record)
    type.nullable? && record[:header][:field_nulls][position]
  end

  # Return the actual length of this variable-length field.
  def length(record)
    if type.variable?
      record[:header][:field_lengths][position]
    else
      type.length
    end
  end

  # Read an InnoDB encoded data field.
  def read(record, cursor)
    return :NULL if null?(record)
    cursor.name(type.name) { type.reader.read(record, cursor, length(record)) }
  end
end
