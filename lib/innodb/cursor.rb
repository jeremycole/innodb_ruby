class Innodb::Cursor
  attr_reader :offset

  def initialize(buffer, offset)
    @buffer = buffer
    @cursor = [ offset ]
    @direction = :forward
  end

  def forward
    @direction = :forward
    self
  end

  def backward
    @direction = :backward
    self
  end

  def position
    @cursor[0]
  end

  def seek(offset)
    @cursor[0] = offset if offset
    self
  end

  def adjust(relative_offset)
    @cursor[0] += relative_offset
    self
  end

  def push(offset=nil)
    @cursor.unshift(offset.nil? ? @cursor[0] : offset)
    self
  end

  def pop
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

  def get_bytes(length)
    read_and_advance(length)
  end

  def get_uint8(offset=nil)
    seek(offset)
    read_and_advance(1, "C")
  end

  def get_uint16(offset=nil)
    seek(offset)
    read_and_advance(2, "n")
  end

  def get_sint16(offset=nil)
    seek(offset)
    uint = read_and_advance(2, "n")
    (uint & 32768) == 0 ? uint : -(uint ^ 65535) - 1
  end

  def get_uint24(offset=nil)
    seek(offset)
    # Ruby 1.8 doesn't support big-endian 24-bit unpack; unpack as one
    # 8-bit and one 16-bit big-endian instead.
    high, low = read_and_advance(3).unpack("nC")
    (high << 8) | low
  end

  def get_uint32(offset=nil)
    seek(offset)
    read_and_advance(4, "N")
  end

  def get_uint64(offset=nil)
    seek(offset)
    # Ruby 1.8 doesn't support big-endian quad-word unpack; unpack as two
    # 32-bit big-endian instead.
    high, low = read_and_advance(8).unpack("NN")
    (high << 32) | low
  end

  # InnoDB compressed 32-bit unsigned integer.
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

end