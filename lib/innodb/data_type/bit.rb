# frozen_string_literal: true

module Innodb
  class DataType
    # MySQL's Bit-Value Type (BIT).
    class Bit < DataType
      specialization_for :BIT

      include HasNumericModifiers

      DEFAULT_SIZE = 1
      SUPPORTED_SIZE_RANGE = (1..64).freeze

      def initialize(type_name, modifiers, properties)
        super

        @size = @modifiers.fetch(0, DEFAULT_SIZE)
        raise "Unsupported width for #{@type_name} type" unless SUPPORTED_SIZE_RANGE.include?(@size)
      end

      def value(data)
        "0b%b" % BinData.const_get("Uint%dbe" % Innodb::DataType.ceil_to(@size, 8)).read(data)
      end

      def length
        @length = Innodb::DataType.ceil_to(@size, 8) / 8
      end
    end
  end
end
