# frozen_string_literal: true

require "innodb/data_dictionary/index_column_references"

module Innodb
  class DataDictionary
    class Index
      attr_reader :name
      attr_reader :type
      attr_reader :tablespace
      attr_reader :root_page_number
      attr_reader :table
      attr_reader :innodb_index_id
      attr_reader :column_references

      def initialize(name:, type:, table:, tablespace:, root_page_number:, innodb_index_id: nil)
        @name = name
        @type = type
        @table = table
        @tablespace = tablespace
        @root_page_number = root_page_number
        @innodb_index_id = innodb_index_id
        @column_references = IndexColumnReferences.new
      end

      def record_describer
        describer = Innodb::RecordDescriber.new

        describer.type(type)

        column_references.each do |column_reference|
          case column_reference.usage
          when :key
            describer.key(column_reference.column.name, *column_reference.column.description)
          when :row
            describer.row(column_reference.column.name, *column_reference.column.description)
          end
        end

        describer
      end
    end
  end
end
