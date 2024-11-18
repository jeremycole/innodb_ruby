# frozen_string_literal: true

module Innodb
  class DataType
    # Fixed-length character type.
    class Character < DataType
      specialization_for :CHAR
      specialization_for :VARCHAR
      specialization_for :BINARY
      specialization_for :VARBINARY

      include HasNumericModifiers

      VALID_LENGTH_RANGE = (0..65_535).freeze # 1..255 characters, up to 4 bytes each
      DEFAULT_LENGTH = 1

      def initialize(type_name, modifiers, properties)
        super

        @variable = false
        @binary = false

        if %i[VARCHAR VARBINARY].include?(@type_name)
          @variable = true
          if @modifiers.empty?
            raise InvalidSpecificationError, "Missing length specification for variable-length type #{@type_name}"
          elsif @modifiers.size > 1
            raise InvalidSpecificationError, "Invalid length specification for variable-length type #{@type_name}"
          end
        end

        @binary = true if %i[BINARY VARBINARY].include?(@type_name)

        @length = @modifiers.fetch(0, DEFAULT_LENGTH)
        return if VALID_LENGTH_RANGE.include?(@length)

        raise InvalidSpecificationError, "Length #{@length} out of range for #{@type_name}"
      end

      def variable?
        @variable
      end

      def value(data)
        # The SQL standard defines that CHAR fields should have end-spaces
        # stripped off.
        @binary ? data : data.sub(/ +$/, "")
      end

      attr_reader :length
    end
  end
end
