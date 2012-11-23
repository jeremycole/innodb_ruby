# A cursor to walk through InnoDB data structures to read fields.
class Innodb::Cursor
  def initialize(buffer, offset)
    @buffer = buffer
    @cursor = [ offset ]
    @direction = :forward
  end

  # Set the direction of the cursor to "forward".
  def forward
    @direction = :forward
    self
  end

  # Set the direction of the cursor to "backward".
  def backward
    @direction = :backward
    self
  end

  # Return the position of the current cursor.
  def position
    @cursor[0]
  end

  # Move the current cursor to a new absolute position.
  def seek(offset)
    @cursor[0] = offset if offset
    self
  end

  # Adjust the current cursor to a new relative position.
  def adjust(relative_offset)
    @cursor[0] += relative_offset
    self
  end

  # Save the current cursor position and start a new (nested, stacked) cursor.
  def push(offset=nil)
    @cursor.unshift(offset.nil? ? @cursor[0] : offset)
    self
  end

  # Restore the last cursor position.
  def pop
    raise "No cursors to pop" unless @cursor.size > 1
    @cursor.shift
    self
  end

  # Execute a block and restore the cursor to the previous position after
  # the block returns. Return the block's return value after restoring the
  # cursor.
  def peek
    raise "No block given" unless block_given?
    push
    result = yield
    pop
    result
  end

  # Read a number of bytes forwards or backwards from the current cursor
  # position and adjust the cursor position by that amount, optionally
  # unpacking the data using the provided type.
  def read_and_advance(length, type=nil)
    data = nil
    #print "data(#{@cursor[0]}..."
    case @direction
    when :forward
      data = @buffer.data(@cursor[0], length)
      adjust(length)
    when :backward
      adjust(-length)
      data = @buffer.data(@cursor[0], length)
    end
    #puts "#{@cursor[0]}) = #{data.bytes.map { |n| "%02x" % n }.join}"
    type ? data.unpack(type).first : data
  end

  # Return raw bytes.
  def get_bytes(length)
    read_and_advance(length)
  end

  # Return raw bytes as hex.
  def get_hex(length)
    read_and_advance(length).bytes.map { |c| "%02x" % c }.join
  end

  # Return a big-endian unsigned 8-bit integer.
  def get_uint8(offset=nil)
    seek(offset)
    read_and_advance(1, "C")
  end

  # Return a big-endian unsigned 16-bit integer.
  def get_uint16(offset=nil)
    seek(offset)
    read_and_advance(2, "n")
  end

  # Return a big-endian signed 16-bit integer.
  def get_sint16(offset=nil)
    seek(offset)
    uint = read_and_advance(2, "n")
    (uint & 32768) == 0 ? uint : -(uint ^ 65535) - 1
  end

  # Return a big-endian unsigned 24-bit integer.
  def get_uint24(offset=nil)
    seek(offset)
    # Ruby 1.8 doesn't support big-endian 24-bit unpack; unpack as one
    # 8-bit and one 16-bit big-endian instead.
    high, low = read_and_advance(3).unpack("nC")
    (high << 8) | low
  end

  # Return a big-endian unsigned 32-bit integer.
  def get_uint32(offset=nil)
    seek(offset)
    read_and_advance(4, "N")
  end

  # Return a big-endian unsigned 64-bit integer.
  def get_uint64(offset=nil)
    seek(offset)
    # Ruby 1.8 doesn't support big-endian quad-word unpack; unpack as two
    # 32-bit big-endian instead.
    high, low = read_and_advance(8).unpack("NN")
    (high << 32) | low
  end

  # Return an InnoDB-compressed unsigned 32-bit integer.
  def get_ic_uint32
    flag = peek { get_uint8 }

    case
    when flag < 0x80
      get_uint8
    when flag < 0xc0
      get_uint16 & 0x7fff
    when flag < 0xe0
      get_uint24 & 0x3fffff
    when flag < 0xf0
      get_uint32 & 0x1fffffff
    when flag == 0xf0
      adjust(+1) # Skip the flag.
      get_uint32
    else
      raise "Invalid flag #{flag.to_s(16)} seen"
    end
  end

  # Return an InnoDB-munged signed 8-bit integer. (This is only implemented
  # for positive integers at the moment.)
  def get_i_sint8
    get_uint8 ^ (1 << 7)
  end

  # Return an InnoDB-munged signed 64-bit integer. (This is only implemented
  # for positive integers at the moment.)
  def get_i_sint64
    get_uint64 ^ (1 << 63)
  end
end