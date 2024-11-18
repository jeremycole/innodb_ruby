# frozen_string_literal: true

require "innodb/data_type"

# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records, including the length
# of the fixed-width and variable-width portion of the field.
module Innodb
  class Field
    ExternReference = Struct.new(
      :space_id,
      :page_number,
      :offset,
      :length, # rubocop:disable Lint/StructNewOverride
      keyword_init: true
    )

    attr_reader :position
    attr_reader :name
    attr_reader :data_type
    attr_reader :nullable

    # Size of a reference to data stored externally to the page.
    EXTERN_FIELD_SIZE = 20

    def initialize(position, name, type_definition, *properties)
      @position = position
      @name = name
      @nullable = !properties.delete(:NOT_NULL)
      @data_type = Innodb::DataType.parse(type_definition, properties)
    end

    # Return whether this field can be NULL.
    def nullable?
      @nullable
    end

    # Return whether this field is NULL.
    def null?(record)
      nullable? && record.header.nulls.include?(@name)
    end

    # Return whether a part of this field is stored externally (off-page).
    def extern?(record)
      record.header.externs.include?(@name)
    end

    def variable?
      @data_type.variable?
    end

    def fixed?
      !variable?
    end

    def blob?
      @data_type.blob?
    end

    # Return the actual length of this variable-length field.
    def length(record)
      if record.header.lengths.include?(@name)
        len = record.header.lengths[@name]
        if fixed? && len != @data_type.length
          raise "Fixed-length mismatch; #{len} vs #{@data_type.length} for #{@data_type.name}"
        end
      else
        len = @data_type.length
      end
      extern?(record) ? len - EXTERN_FIELD_SIZE : len
    end

    # Read an InnoDB encoded data field.
    def read(cursor, field_length)
      cursor.name(@data_type.name) { cursor.read_bytes(field_length) }
    end

    def value_by_length(cursor, field_length)
      @data_type.value(read(cursor, field_length))
    end

    # Read the data value (e.g. encoded in the data).
    def value(cursor, record)
      return :NULL if null?(record)

      value_by_length(cursor, length(record))
    end

    # Read an InnoDB external pointer field.
    def extern(cursor, record)
      return unless extern?(record)

      cursor.name(@name) { read_extern(cursor) }
    end

    private

    # Return an external reference field. An extern field contains the page
    # address and the length of the externally stored part of the record data.
    def read_extern(cursor)
      cursor.name("extern") do |c|
        ExternReference.new(
          space_id: c.name("space_id") { c.read_uint32 },
          page_number: c.name("page_number") { c.read_uint32 },
          offset: c.name("offset") { c.read_uint32 },
          length: c.name("length") { c.read_uint64 & 0x3fffffff }
        )
      end
    end
  end
end
