# frozen_string_literal: true

module Innodb
  class DataDictionary
    class Column
      attr_reader :name
      attr_reader :description
      attr_reader :table

      def initialize(name:, description:, table:)
        @name = name
        @description = description
        @table = table
      end
    end
  end
end
