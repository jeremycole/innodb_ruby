# frozen_string_literal: true

require "innodb/data_dictionary/columns"
require "innodb/data_dictionary/indexes"

module Innodb
  class DataDictionary
    class Table
      attr_reader :name
      attr_reader :tablespace
      attr_reader :innodb_table_id
      attr_reader :columns
      attr_reader :indexes

      def initialize(name:, tablespace: nil, innodb_table_id: nil)
        @name = name
        @tablespace = tablespace
        @innodb_table_id = innodb_table_id
        @columns = Columns.new
        @indexes = Indexes.new
      end

      def clustered_index
        indexes.find(type: :clustered)
      end
    end
  end
end
