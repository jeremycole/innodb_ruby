# -*- encoding : utf-8 -*-
require "innodb/data_type"

# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records, including the length
# of the fixed-width and variable-width portion of the field.
class Innodb::Field
  attr_reader :position, :data_type

  # Size of a reference to data stored externally to the page.
  EXTERN_FIELD_SIZE = 20

  def initialize(position, data_type, *properties)
    @position = position
    @data_type = Innodb::DataType.new(data_type.to_s, properties)
  end

  # Return whether this field is NULL.
  def null?(record)
    @data_type.nullable? && record[:header][:field_nulls][position]
  end

  # Return whether a part of this field is stored externally (off-page).
  def extern?(record)
    record[:header][:field_externs][position]
  end

  # Return the actual length of this variable-length field.
  def length(record)
    if @data_type.variable?
      len = record[:header][:field_lengths][position]
    else
      len = @data_type.length
    end
    extern?(record) ? len - EXTERN_FIELD_SIZE : len
  end

  # Read an InnoDB encoded data field.
  def read(record, cursor)
    return :NULL if null?(record)
    cursor.name(@data_type.name) { @data_type.reader.read(cursor, length(record)) }
  end

  # Read an InnoDB external pointer field.
  def read_extern(record, cursor)
    return nil if not extern?(record)
    cursor.name(@data_type.name) { @data_type.reader.read_extern(cursor) }
  end
end
