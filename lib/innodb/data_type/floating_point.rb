# frozen_string_literal: true

module Innodb
  class DataType
    class FloatingPoint < DataType
      specialization_for :FLOAT
      specialization_for :DOUBLE

      include HasNumericModifiers

      def value(data)
        case type_name
        when :FLOAT
          BinData::FloatLe.read(data)
        when :DOUBLE
          BinData::DoubleLe.read(data)
        end
      end

      def length
        case type_name
        when :FLOAT
          4
        when :DOUBLE
          8
        end
      end
    end
  end
end
