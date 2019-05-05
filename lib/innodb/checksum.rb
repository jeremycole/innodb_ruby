# -*- encoding : utf-8 -*-

class Innodb::Checksum
  MAX   = 0xFFFFFFFF.freeze
  MASK1 = 1463735687.freeze
  MASK2	= 1653893711.freeze

  # This is derived from ut_fold_ulint_pair in include/ut0rnd.ic in the
  # InnoDB source code. Since Ruby's Bignum class is *much* slower than its
  # Integer class, we mask back to 32 bits to keep things from overflowing
  # and being promoted to Bignum.
  def self.fold_pair(n1, n2)
    (((((((n1 ^ n2 ^ MASK2) << 8) & MAX) + n1) & MAX) ^ MASK1) + n2) & MAX
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
