module ReadBitsAtOffset
  # Read a given number of bits from an integer at a specific bit offset. The
  # value returned is 0-based so does not need further shifting or adjustment.
  def read_bits_at_offset(data, bits, offset)
    ((data & (((1 << bits) - 1) << offset)) >> offset)
  end
end

