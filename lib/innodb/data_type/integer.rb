# frozen_string_literal: true

module Innodb
  class DataType
    class Integer < DataType
      specialization_for :BOOL
      specialization_for :BOOLEAN
      specialization_for :TINYINT
      specialization_for :SMALLINT
      specialization_for :MEDIUMINT
      specialization_for :INT
      specialization_for :INT6
      specialization_for :BIGINT

      include HasNumericModifiers

      TYPE_BIT_LENGTH_MAP = {
        BOOL: 8,
        BOOLEAN: 8,
        TINYINT: 8,
        SMALLINT: 16,
        MEDIUMINT: 24,
        INT: 32,
        INT6: 48,
        BIGINT: 64,
      }.freeze

      def initialize(type_name, modifiers, properties)
        super

        @unsigned = properties&.include?(:UNSIGNED)
      end

      def bit_length
        @bit_length ||= TYPE_BIT_LENGTH_MAP[type_name]
      end

      def unsigned?
        @unsigned
      end

      def value(data)
        if unsigned?
          BinData.const_get("Uint%dbe" % bit_length).read(data)
        else
          BinData.const_get("Int%dbe" % bit_length).read(data) ^ (-1 << (bit_length - 1))
        end
      end

      def length
        bit_length / 8
      end
    end
  end
end
