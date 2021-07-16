# frozen_string_literal: true

require 'innodb/data_type'

# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records, including the length
# of the fixed-width and variable-width portion of the field.
module Innodb
  class Field
    ExternReference = Struct.new(
      :space_id,
      :page_number,
      :offset,
      :length,
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
      base_type, modifiers = parse_type_definition(type_definition.to_s)
      @data_type = Innodb::DataType.new(base_type, modifiers, properties)
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
      [
        Innodb::DataType::BlobType,
        Innodb::DataType::VariableBinaryType,
        Innodb::DataType::VariableCharacterType,
      ].any? { |c| @data_type.is_a?(c) }
    end

    def blob?
      @data_type.is_a?(Innodb::DataType::BlobType)
    end

    # Return the actual length of this variable-length field.
    def length(record)
      if record.header.lengths.include?(@name)
        len = record.header.lengths[@name]
        raise 'Fixed-length mismatch' unless variable? || len == @data_type.width
      else
        len = @data_type.width
      end
      extern?(record) ? len - EXTERN_FIELD_SIZE : len
    end

    # Read an InnoDB encoded data field.
    def read(cursor, field_length)
      cursor.name(@data_type.name) { cursor.read_bytes(field_length) }
    end

    def value_by_length(cursor, field_length)
      if @data_type.respond_to?(:read)
        cursor.name(@data_type.name) { @data_type.read(cursor) }
      elsif @data_type.respond_to?(:value)
        @data_type.value(read(cursor, field_length))
      else
        read(cursor, field_length)
      end
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
      cursor.name('extern') do |c|
        ExternReference.new(
          space_id: c.name('space_id') { c.read_uint32 },
          page_number: c.name('page_number') { c.read_uint32 },
          offset: c.name('offset') { c.read_uint32 },
          length: c.name('length') { c.read_uint64 & 0x3fffffff }
        )
      end
    end

    # Parse a data type definition and extract the base type and any modifiers.
    def parse_type_definition(type_string)
      matches = /^([a-zA-Z0-9_]+)(\(([0-9, ]+)\))?$/.match(type_string)
      return unless matches

      base_type = matches[1].upcase.to_sym
      return [base_type, []] unless matches[3]

      [base_type, matches[3].sub(/ /, '').split(/,/).map(&:to_i)]
    end
  end
end
