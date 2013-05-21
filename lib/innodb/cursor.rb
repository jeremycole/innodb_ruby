# -*- encoding : utf-8 -*-
require "bindata"

# A cursor to walk through InnoDB data structures to read fields.
class Innodb::Cursor

  # An entry in a stack of cursors. The cursor position, direction, and
  # name array are each attributes of the current cursor stack and are
  # manipulated together.
  class StackEntry
    attr_accessor :cursor
    attr_accessor :position
    attr_accessor :direction
    attr_accessor :name

    def initialize(cursor, position=0, direction=:forward, name=nil)
      @cursor = cursor
      @position = position
      @direction = direction
      @name = name || []
    end

    def dup
      StackEntry.new(cursor, position, direction, name.dup)
    end
  end

  @@tracing = false

  # Enable tracing for all Innodb::Cursor objects.
  def self.trace!(arg=true)
    @@tracing = arg
  end

  # Initialize a cursor within a buffer at the given position.
  def initialize(buffer, position)
    @buffer = buffer
    @stack = [ StackEntry.new(self, position) ]

    trace_with :print_trace
  end

  # Print a trace output for this cursor. The method is passed a cursor object,
  # position, raw byte buffer, and array of names.
  def print_trace(cursor, position, bytes, name)
    slice_size = 16
    bytes.each_slice(slice_size).each_with_index do |slice_bytes, slice_count|
      puts "%06i %s %-32s  %s" % [
        position + (slice_count * slice_size),
        direction == :backward ? "←" : "→",
        slice_bytes.map { |n| "%02x" % n }.join,
        slice_count == 0 ? name.join(".") : "↵",
      ]
    end
  end

  # Set a Proc or method on self to trace with.
  def trace_with(arg=nil)
    if arg.nil?
      @trace_proc = nil
    elsif arg.class == Proc
      @trace_proc = arg
    elsif arg.class == Symbol
      @trace_proc = lambda { |cursor, position, bytes, name| self.send(arg, cursor, position, bytes, name) }
    else
      raise "Don't know how to trace with #{arg}"
    end
  end

  # Generate a trace record from the current cursor.
  def trace(position, bytes, name)
    @trace_proc.call(self, position, bytes, name) if @@tracing && @trace_proc
  end

  # The current cursor object; the top of the stack.
  def current
    @stack.last
  end

  # Set the field name.
  def name(name_arg=nil)
    if name_arg.nil?
      return current.name
    end

    unless block_given?
      raise "No block given"
    end

    current.name.push name_arg
    ret = yield(self)
    current.name.pop
    ret
  end

  # Return the direction of the current cursor.
  def direction(direction_arg=nil)
    if direction_arg.nil?
      return current.direction
    end

    current.direction = direction_arg
    self
  end

  # Set the direction of the cursor to "forward".
  def forward
    direction(:forward)
  end

  # Set the direction of the cursor to "backward".
  def backward
    direction(:backward)
  end

  # Return the position of the current cursor.
  def position
    current.position
  end

  # Move the current cursor to a new absolute position.
  def seek(position)
    current.position = position if position
    self
  end

  # Adjust the current cursor to a new relative position.
  def adjust(relative_position)
    current.position += relative_position
    self
  end

  # Save the current cursor position and start a new (nested, stacked) cursor.
  def push(position=nil)
    @stack.push current.dup
    seek(position)
    self
  end

  # Restore the last cursor position.
  def pop
    raise "No cursors to pop" unless @stack.size > 1
    @stack.pop
    self
  end

  # Execute a block and restore the cursor to the previous position after
  # the block returns. Return the block's return value after restoring the
  # cursor. Optionally seek to provided position before executing block.
  def peek(position=nil)
    raise "No block given" unless block_given?
    push(position)
    result = yield(self)
    pop
    result
  end

  # Read a number of bytes forwards or backwards from the current cursor
  # position and adjust the cursor position by that amount.
  def read_and_advance(length)
    data = nil
    cursor_start = current.position
    case current.direction
    when :forward
      data = @buffer.data(current.position, length)
      adjust(length)
    when :backward
      adjust(-length)
      data = @buffer.data(current.position, length)
    end

    trace(cursor_start, data.bytes, current.name)
    data
  end

  # Return raw bytes.
  def get_bytes(length)
    read_and_advance(length)
  end

  def each_byte_as_uint8(length)
    unless block_given?
      return enum_for(:each_byte_as_uint8, length)
    end

    read_and_advance(length).bytes.each do |byte|
      yield byte
    end

    nil
  end

  # Return raw bytes as hex.
  def get_hex(length)
    read_and_advance(length).bytes.map { |c| "%02x" % c }.join
  end

  # Read an unsigned 8-bit integer.
  def get_uint8(position=nil)
    seek(position)
    data = read_and_advance(1)
    BinData::Uint8.read(data)
  end

  # Read a big-endian unsigned 16-bit integer.
  def get_uint16(position=nil)
    seek(position)
    data = read_and_advance(2)
    BinData::Uint16be.read(data)
  end

  # Read a big-endian signed 16-bit integer.
  def get_sint16(position=nil)
    seek(position)
    data = read_and_advance(2)
    BinData::Int16be.read(data)
  end

  # Read a big-endian unsigned 24-bit integer.
  def get_uint24(position=nil)
    seek(position)
    data = read_and_advance(3)
    BinData::Uint24be.read(data)
  end

  # Read a big-endian unsigned 32-bit integer.
  def get_uint32(position=nil)
    seek(position)
    data = read_and_advance(4)
    BinData::Uint32be.read(data)
  end

  # Read a big-endian unsigned 48-bit integer.
  def get_uint48(position=nil)
    seek(position)
    data = read_and_advance(6)
    BinData::Uint48be.read(data)
  end

  # Read a big-endian unsigned 64-bit integer.
  def get_uint64(position=nil)
    seek(position)
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
    when 6
      get_uint48
    when 8
      get_uint64
    else
      raise "Not implemented"
    end
  end

  def get_uint_array_by_size(size, count)
    (0...count).to_a.inject([]) { |a, n| a << get_uint_by_size(size); a }
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

  # Read an InnoDB-munged signed 48-bit integer.
  def get_i_sint48
    data = read_and_advance(6)
    BinData::Int48be.read(data) ^ (-1 << 47)
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
    when 6
      get_i_sint48
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
