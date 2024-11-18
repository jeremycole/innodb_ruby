# frozen_string_literal: true

require "bigdecimal"
require "stringio"

module Innodb
  class DataType
    # MySQL's Fixed-Point Type (DECIMAL), stored in InnoDB as a binary string.
    class Decimal < DataType
      specialization_for :DECIMAL
      specialization_for :NUMERIC

      include HasNumericModifiers

      # The value is stored as a sequence of signed big-endian integers, each
      # representing up to 9 digits of the integral and fractional parts. The
      # first integer of the integral part and/or the last integer of the
      # fractional part might be compressed (or packed) and are of variable
      # length. The remaining integers (if any) are uncompressed and 32 bits
      # wide.
      MAX_DIGITS_PER_INTEGER = 9
      BYTES_PER_DIGIT = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4].freeze

      DEFAULT_PRECISION = 10
      VALID_PRECISION_RANGE = (1..65).freeze

      DEFAULT_SCALE = 0
      VALID_SCALE_RANGE = (0..30).freeze

      def self.length_attributes(precision, scale)
        integral = precision - scale

        integral_count_full_parts = integral / MAX_DIGITS_PER_INTEGER
        integral_first_part_length = integral - (integral_count_full_parts * MAX_DIGITS_PER_INTEGER)

        fractional_count_full_parts = scale / MAX_DIGITS_PER_INTEGER
        fractional_first_part_length = scale - (fractional_count_full_parts * MAX_DIGITS_PER_INTEGER)

        integral_length = (integral_count_full_parts * 4) + BYTES_PER_DIGIT[integral_first_part_length]
        fractional_length = (fractional_count_full_parts * 4) + BYTES_PER_DIGIT[fractional_first_part_length]

        {
          length: integral_length + fractional_length,
          integral: {
            length: integral_length,
            first_part_length: integral_first_part_length,
            count_full_parts: integral_count_full_parts,
          },
          fractional: {
            length: fractional_length,
            first_part_length: fractional_first_part_length,
            count_full_parts: fractional_count_full_parts,
          },
        }
      end

      def initialize(type_name, modifiers, properties)
        super

        raise "Invalid #{@type_name} specification: #{@modifiers}" unless @modifiers.size <= 2

        @precision = @modifiers.fetch(0, DEFAULT_PRECISION)
        @scale = @modifiers.fetch(1, DEFAULT_SCALE)

        unless VALID_PRECISION_RANGE.include?(@precision)
          raise "Unsupported precision #{@precision} for #{@type_name} type"
        end

        unless VALID_SCALE_RANGE.include?(@scale) && @scale <= @precision
          raise "Unsupported scale #{@scale} for #{@type_name} type"
        end

        @length_attributes = self.class.length_attributes(@precision, @scale)
      end

      def length
        @length_attributes[:length]
      end

      def value(data)
        # Strings representing the integral and fractional parts.
        intg = "".dup
        frac = "".dup

        stream = StringIO.new(data)
        mask = sign_mask(stream)

        intg << get_digits(stream, mask, @length_attributes[:integral][:first_part_length])

        @length_attributes[:integral][:count_full_parts].times do
          intg << get_digits(stream, mask, MAX_DIGITS_PER_INTEGER)
        end

        @length_attributes[:fractional][:count_full_parts].times do
          frac << get_digits(stream, mask, MAX_DIGITS_PER_INTEGER)
        end

        frac << get_digits(stream, mask, @length_attributes[:fractional][:first_part_length])
        frac = "0" if frac.empty?

        # Convert to something resembling a string representation.
        str = "#{mask.to_s.chop}#{intg}.#{frac}"

        BigDecimal(str).to_s("F")
      end

      private

      # The sign is encoded in the high bit of the first byte/digit. The byte
      # might be part of a larger integer, so apply the bit-flipper and push
      # back the byte into the stream.
      def sign_mask(stream)
        byte = BinData::Uint8.read(stream)
        sign = byte & 0x80
        byte.assign(byte ^ 0x80)
        stream.rewind
        byte.write(stream)
        stream.rewind
        sign.zero? ? -1 : 0
      end

      # Return a string representing an integer with a specific number of digits.
      def get_digits(stream, mask, digits)
        nbits = BYTES_PER_DIGIT[digits] * 8
        return "" unless nbits.positive?

        value = (BinData.const_get("Int%dbe" % nbits).read(stream) ^ mask)
        # Preserve leading zeros.
        "%0#{digits}d" % value
      end
    end
  end
end
