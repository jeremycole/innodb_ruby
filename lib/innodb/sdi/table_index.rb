# frozen_string_literal: true

module Innodb
  class Sdi
    class TableIndex
      TYPES = {
        1 => :PRIMARY,
        2 => :UNIQUE,
        3 => :MULTIPLE,
        4 => :FULLTEXT,
        5 => :SPATIAL,
      }.freeze

      ALGORITHMS = {
        1 => :SE_SPECIFIC,
        2 => :BTREE,
        3 => :RTREE,
        4 => :HASH,
        5 => :FULLTEXT,
      }.freeze

      attr_reader :table
      attr_reader :data

      def initialize(table, data)
        @table = table
        @data = data
      end

      def name
        data["name"]
      end

      def hidden?
        data["hidden"]
      end

      def visible?
        data["is_visible"]
      end

      def generated?
        data["is_generated"]
      end

      def elements
        data["elements"]
          .map { |element| TableIndexElement.new(self, element) }
          .sort_by(&:ordinal_position)
      end

      def se_private_data
        data["se_private_data"]&.split(";").to_h { |x| x.split("=") }
      end

      def index_id
        se_private_data["id"].to_i
      end

      def root_page_number
        se_private_data["root"].to_i
      end

      def space_id
        se_private_data["space_id"].to_i
      end

      def table_id
        se_private_data["table_id"].to_i
      end

      def trx_id
        se_private_data["trx_id"].to_i
      end

      def options
        data["options"]&.split(";").to_h { |x| x.split("=") }
      end

      def type
        TYPES[data["type"]]
      end

      def primary?
        type == :PRIMARY
      end

      def clustered?
        table.clustered_index.index_id == index_id
      end

      def record_describer
        describer = Innodb::RecordDescriber.new
        describer.type(clustered? ? :clustered : :secondary)

        elements.each do |element|
          if element.key?
            describer.key(element.column.name, *element.column.description)
          elsif element.row?
            describer.row(element.column.name, *element.column.description)
          end
        end

        describer
      end
    end
  end
end
