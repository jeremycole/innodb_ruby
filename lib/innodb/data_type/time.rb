# frozen_string_literal: true

module Innodb
  class DataType
    class Time < DataType
      specialization_for :TIME

      include HasNumericModifiers

      def value(data)
        time = BinData::Int24be.read(data) ^ (-1 << 23)
        sign = "-" if time.negative?
        time = time.abs
        "%s%02d:%02d:%02d" % [sign, time / 10_000, (time / 100) % 100, time % 100]
      end

      def length
        3
      end
    end
  end
end
