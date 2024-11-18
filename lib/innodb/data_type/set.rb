# frozen_string_literal: true

module Innodb
  class DataType
    class Set < DataType
      specialization_for :SET

      include HasStringListModifiers

      attr_reader :values

      def initialize(type_name, modifiers, properties)
        super

        @values = @modifiers.each_with_index.to_h { |s, i| [2**i, s] }
      end

      def bit_length
        @bit_length ||= Innodb::DataType.ceil_to(@values.length, 8)
      end

      def value(data)
        bitmap = BinData.const_get("Int%dbe" % bit_length).read(data)
        (0...bit_length).map { |i| bitmap & (2**i) }.reject(&:zero?).map { |i| values[i] }
      end

      def length
        bit_length / 8
      end
    end
  end
end
