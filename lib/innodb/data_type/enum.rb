# frozen_string_literal: true

module Innodb
  class DataType
    class Enum < DataType
      specialization_for :ENUM

      include HasStringListModifiers

      attr_reader :values

      def initialize(type_name, modifiers, properties)
        super

        @values = { 0 => "" }
        @values.merge!(@modifiers.each_with_index.to_h { |s, i| [i + 1, s] })
      end

      def bit_length
        @bit_length ||= Innodb::DataType.ceil_to(Math.log2(@values.length).ceil, 8)
      end

      def value(data)
        index = BinData.const_get("Int%dbe" % bit_length).read(data)
        values[index]
      end

      def length
        bit_length / 8
      end
    end
  end
end
