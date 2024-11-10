# frozen_string_literal: true

require "forwardable"

module Innodb
  class DataDictionary
    class IndexColumnReference
      extend Forwardable

      attr_reader :column
      attr_reader :usage
      attr_reader :index

      def_delegators :column, :name, :description, :table

      def initialize(column:, usage:, index:)
        @column = column
        @usage = usage
        @index = index
      end
    end
  end
end
