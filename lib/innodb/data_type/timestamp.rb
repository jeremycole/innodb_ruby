# frozen_string_literal: true

require "date"

module Innodb
  class DataType
    class Timestamp < DataType
      specialization_for :TIMESTAMP

      include HasNumericModifiers

      # Returns the UTC timestamp as a value in 'YYYY-MM-DD HH:MM:SS' format.
      def value(data)
        timestamp = BinData::Uint32be.read(data)
        return "0000-00-00 00:00:00" if timestamp.zero?

        DateTime.strptime(timestamp.to_s, "%s").strftime "%Y-%m-%d %H:%M:%S"
      end

      def length
        4
      end
    end
  end
end
