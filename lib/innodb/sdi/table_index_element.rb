# frozen_string_literal: true

module Innodb
  class Sdi
    class TableIndexElement
      attr_reader :index
      attr_reader :data

      def initialize(index, data)
        @index = index
        @data = data
      end

      def ordinal_position
        data["ordinal_position"]
      end

      def length
        data["length"]
      end

      def order
        data["order"]
      end

      def hidden?
        data["hidden"]
      end

      def visible?
        !hidden?
      end

      def key?
        visible?
      end

      def row?
        hidden?
      end

      def column_opx
        data["column_opx"]
      end

      def column
        index.table.columns[column_opx]
      end
    end
  end
end
