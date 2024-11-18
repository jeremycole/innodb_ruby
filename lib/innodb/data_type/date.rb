# frozen_string_literal: true

module Innodb
  class DataType
    class Date < DataType
      specialization_for :DATE

      include HasNumericModifiers

      def value(data)
        date = BinData::Int24be.read(data) ^ (-1 << 23)
        day = date & 0x1f
        month = (date >> 5) & 0xf
        year = date >> 9
        "%04d-%02d-%02d" % [year, month, day]
      end

      def length
        3
      end
    end
  end
end
