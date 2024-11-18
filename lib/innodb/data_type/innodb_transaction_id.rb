# frozen_string_literal: true

module Innodb
  class DataType
    # Transaction ID.
    class InnodbTransactionId < DataType
      specialization_for :TRX_ID

      def value(data)
        BinData::Uint48be.read(data).to_i
      end

      def length
        6
      end
    end
  end
end
