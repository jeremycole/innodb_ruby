# frozen_string_literal: true

module Innodb
  class DataType
    class Datetime < DataType
      specialization_for :DATETIME

      include HasNumericModifiers

      def value(data)
        datetime = BinData::Int64be.read(data) ^ (-1 << 63)
        date = datetime / 1_000_000
        year = date / 10_000
        month = (date / 100) % 100
        day = date % 100
        time = datetime - (date * 1_000_000)
        hour = time / 10_000
        min = (time / 100) % 100
        sec = time % 100
        "%04d-%02d-%02d %02d:%02d:%02d" % [year, month, day, hour, min, sec]
      end

      def length
        8
      end
    end
  end
end
