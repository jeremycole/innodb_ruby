# frozen_string_literal: true

module Innodb
  class Checksum
    MAX   = 0xFFFFFFFF
    MASK1 = 1_463_735_687
    MASK2	= 1_653_893_711

    # This is derived from ut_fold_ulint_pair in include/ut0rnd.ic in the
    # InnoDB source code. Since Ruby's Bignum class is *much* slower than its
    # Integer class, we mask back to 32 bits to keep things from overflowing
    # and being promoted to Bignum.
    def self.fold_pair(num1, num2)
      (((((((num1 ^ num2 ^ MASK2) << 8) & MAX) + num1) & MAX) ^ MASK1) + num2) & MAX
    end

    # Iterate through the provided enumerator, which is expected to return a
    # Integer (or something coercible to it), and "fold" them together to produce
    # a single value.
    def self.fold_enumerator(enumerator)
      fold = 0
      enumerator.each do |byte|
        fold = fold_pair(fold, byte)
      end
      fold
    end

    # A simple helper (and example) to fold a provided string.
    def self.fold_string(string)
      fold_enumerator(string.bytes)
    end
  end
end
