# frozen_string_literal: true

require "innodb/data_dictionary/tablespaces"
require "innodb/data_dictionary/tables"

module Innodb
  class DataDictionary
    attr_reader :tablespaces
    attr_reader :tables
    attr_reader :indexes
    attr_reader :columns

    def initialize
      @tablespaces = Tablespaces.new
      @tables = Tables.new
      @indexes = Indexes.new
      @columns = Columns.new
    end

    def inspect
      format("#<%s: %i tablespaces, %i tables, %i indexes, %i columns>",
             self.class.name,
             tablespaces.count,
             tables.count,
             indexes.count,
             columns.count)
    end

    def refresh
      tables.each do |table|
        table.indexes.each do |index|
          indexes.add(index)
        end

        table.columns.each do |column|
          columns.add(column)
        end
      end

      nil
    end

    def populated?
      tablespaces.any? || tables.any?
    end
  end
end
