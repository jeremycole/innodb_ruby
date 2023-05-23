# frozen_string_literal: true

module Innodb
  class Sdi
    class Table < SdiObject
      specialization_for 1

      def name
        format("%s/%s", dd_object["schema_ref"], dd_object["name"])
      end

      def columns
        dd_object["columns"].map { |column| TableColumn.new(self, column) }
      end

      def indexes
        dd_object["indexes"].map { |index| TableIndex.new(self, index) }
      end

      def space_id
        indexes.first.space_id
      end

      def each_column(&block)
        return enum_for(:each_column) unless block_given?

        dd_object["columns"].each(&block)

        nil
      end

      def find_index_by_name(name)
        indexes.find { |index| index.name == name }
      end

      def clustered_index
        indexes.select { |i| %i[PRIMARY UNIQUE].include?(i.type) }.first
      end
    end
  end
end
