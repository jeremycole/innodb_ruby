# -*- encoding : utf-8 -*-
require "innodb/data_type"

# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records, including the length
# of the fixed-width and variable-width portion of the field.
class Innodb::Field
  attr_reader :position, :nullable, :data_type

  # Size of a reference to data stored externally to the page.
  EXTERN_FIELD_SIZE = 20

  def initialize(position, data_type, *properties)
    @position = position
    @nullable = properties.delete(:NOT_NULL) ? false : true
    @data_type = Innodb::DataType.new(data_type.to_s, properties)
  end

  # Return whether this field can be NULL.
  def nullable?
    @nullable
  end

  # Return whether this field is NULL.
  def null?(record)
    nullable? && record[:header][:field_nulls][position]
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
    cursor.name(@data_type.name) { cursor.get_bytes(length(record)) }
  end

  # Read the data value (e.g. encoded in the data).
  def value(record, cursor)
    return :NULL if null?(record)
    data = read(record, cursor)
    @data_type.reader.respond_to?(:value) ? @data_type.reader.value(data) : data
  end

  # Read an InnoDB external pointer field.
  def extern(record, cursor)
    return nil if not extern?(record)
    cursor.name(@data_type.name) { read_extern(cursor) }
  end

  private

  # Return an external reference field. An extern field contains the page
  # address and the length of the externally stored part of the record data.
  def get_extern_reference(cursor)
    {
      :space_id     => cursor.name("space_id")    { cursor.get_uint32 },
      :page_number  => cursor.name("page_number") { cursor.get_uint32 },
      :offset       => cursor.name("offset")      { cursor.get_uint32 },
      :length       => cursor.name("length")      { cursor.get_uint64 & 0x3fffffff }
    }
  end

  def read_extern(cursor)
    cursor.name("extern") { get_extern_reference(cursor) }
  end
end
