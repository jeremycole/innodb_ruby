# frozen_string_literal: true

module Innodb
  class DataType
    # Rollback data pointer.
    class InnodbRollPointer < DataType
      specialization_for :ROLL_PTR

      extend ReadBitsAtOffset

      Pointer = Struct.new(
        :is_insert,
        :rseg_id,
        :undo_log,
        keyword_init: true
      )

      def self.parse_roll_pointer(roll_ptr)
        Pointer.new(
          is_insert: read_bits_at_offset(roll_ptr, 1, 55) == 1,
          rseg_id: read_bits_at_offset(roll_ptr, 7, 48),
          undo_log: Innodb::Page::Address.new(
            page: read_bits_at_offset(roll_ptr, 32, 16),
            offset: read_bits_at_offset(roll_ptr, 16, 0)
          )
        )
      end

      def value(data)
        roll_ptr = BinData::Uint56be.read(data)
        self.class.parse_roll_pointer(roll_ptr)
      end

      def length
        7
      end
    end
  end
end
