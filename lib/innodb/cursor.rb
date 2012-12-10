require "bindata"

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
  # position and adjust the cursor position by that amount.
  def read_and_advance(length)
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
    data
  end

  # Return raw bytes.
  def get_bytes(length)
    read_and_advance(length)
  end

  # Return raw bytes as hex.
  def get_hex(length)
    read_and_advance(length).bytes.map { |c| "%02x" % c }.join
  end

  # Read an unsigned 8-bit integer.
  def get_uint8(offset=nil)
    seek(offset)
    data = read_and_advance(1)
    BinData::Uint8.read(data)
  end

  # Read a big-endian unsigned 16-bit integer.
  def get_uint16(offset=nil)
    seek(offset)
    data = read_and_advance(2)
    BinData::Uint16be.read(data)
  end

  # Read a big-endian signed 16-bit integer.
  def get_sint16(offset=nil)
    seek(offset)
    data = read_and_advance(2)
    BinData::Int16be.read(data)
  end

  # Read a big-endian unsigned 24-bit integer.
  def get_uint24(offset=nil)
    seek(offset)
    data = read_and_advance(3)
    BinData::Uint24be.read(data)
  end

  # Read a big-endian unsigned 32-bit integer.
  def get_uint32(offset=nil)
    seek(offset)
    data = read_and_advance(4)
    BinData::Uint32be.read(data)
  end

  # Read a big-endian unsigned 64-bit integer.
  def get_uint64(offset=nil)
    seek(offset)
    data = read_and_advance(8)
    BinData::Uint64be.read(data)
  end

  # Read a big-endian unsigned integer given its size in bytes.
  def get_uint_by_size(size)
    case size
    when 1
      get_uint8
    when 2
      get_uint16
    when 3
      get_uint24
    when 4
      get_uint32
    when 8
      get_uint64
    else
      raise "Not implemented"
    end
  end

  # Read an InnoDB-compressed unsigned 32-bit integer.
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

  # Read an InnoDB-munged signed 8-bit integer.
  def get_i_sint8
    data = read_and_advance(1)
    BinData::Int8.read(data) ^ (-1 << 7)
  end

  # Read an InnoDB-munged signed 16-bit integer.
  def get_i_sint16
    data = read_and_advance(2)
    BinData::Int16be.read(data) ^ (-1 << 15)
  end

  # Read an InnoDB-munged signed 24-bit integer.
  def get_i_sint24
    data = read_and_advance(3)
    BinData::Int24be.read(data) ^ (-1 << 23)
  end

  # Read an InnoDB-munged signed 32-bit integer.
  def get_i_sint32
    data = read_and_advance(4)
    BinData::Int32be.read(data) ^ (-1 << 31)
  end

  # Read an InnoDB-munged signed 64-bit integer.
  def get_i_sint64
    data = read_and_advance(8)
    BinData::Int64be.read(data) ^ (-1 << 63)
  end

  # Read an InnoDB-munged signed integer given its size in bytes.
  def get_i_sint_by_size(size)
    case size
    when 1
      get_i_sint8
    when 2
      get_i_sint16
    when 3
      get_i_sint24
    when 4
      get_i_sint32
    when 8
      get_i_sint64
    else
      raise "Not implemented"
    end
  end

  # Read an array of 1-bit integers.
  def get_bit_array(num_bits)
    size = (num_bits + 7) / 8
    data = read_and_advance(size)
    bit_array = BinData::Array.new(:type => :bit1, :initial_length => size * 8)
    bit_array.read(data).to_ary
  end
end
