# frozen_string_literal: true

module Innodb
  class DataType
    class Year < DataType
      specialization_for :YEAR

      include HasNumericModifiers

      DEFAULT_DISPLAY_WIDTH = 4
      VALID_DISPLAY_WIDTHS = [2, 4].freeze

      def initialize(type_name, modifiers, properties)
        super

        @display_width = modifiers.fetch(0, DEFAULT_DISPLAY_WIDTH)
        return if VALID_DISPLAY_WIDTHS.include?(@display_width)

        raise InvalidSpecificationError, "Unsupported display width #{@display_width} for type #{type_name}"
      end

      def value(data)
        year = BinData::Uint8.read(data)
        return (year % 100).to_s if @display_width != 4
        return (year + 1900).to_s if year != 0

        "0000"
      end

      def length
        1
      end
    end
  end
end
