# -*- encoding : utf-8 -*-

# An InnoDB transaction log block.
class Innodb::LogBlock
  # Log blocks are fixed-length at 512 bytes in InnoDB.
  BLOCK_SIZE = 512

  # Offset of the header within the log block.
  HEADER_OFFSET = 0

  # The size of the block header.
  HEADER_SIZE = 4 + 2 + 2 + 4

  # Offset of the trailer within ths log block.
  TRAILER_OFFSET = BLOCK_SIZE - 4

  # The size of the block trailer.
  TRAILER_SIZE = 4

  # Offset of the start of data in the block.
  DATA_OFFSET = HEADER_SIZE

  # Size of the space available for log records.
  DATA_SIZE = BLOCK_SIZE - HEADER_SIZE - TRAILER_SIZE

  # Mask used to get the flush bit in the header.
  HEADER_FLUSH_BIT_MASK = 0x80000000

  # Initialize a log block by passing in a 512-byte buffer containing the raw
  # log block contents.
  def initialize(buffer)
    unless buffer.size == BLOCK_SIZE
      raise "Log block buffer provided was not #{BLOCK_SIZE} bytes"
    end

    @buffer = buffer
  end

  # Return an BufferCursor object positioned at a specific offset.
  def cursor(offset)
    BufferCursor.new(@buffer, offset)
  end

  # Return the log block header.
  def header
    @header ||= cursor(HEADER_OFFSET).name("header") do |c|
      {
        :flush => c.name("flush") {
          c.peek { (c.get_uint32 & HEADER_FLUSH_BIT_MASK) > 0 }
        },
        :block_number => c.name("block_number") {
          c.get_uint32 & ~HEADER_FLUSH_BIT_MASK
        },
        :data_length      => c.name("data_length")     { c.get_uint16 },
        :first_rec_group  => c.name("first_rec_group") { c.get_uint16 },
        :checkpoint_no    => c.name("checkpoint_no")   { c.get_uint32 },
      }
    end
  end

  # Return a slice of actual block data (that is, excluding header and
  # trailer) starting at the given offset.
  def data(offset = DATA_OFFSET)
    length = header[:data_length]

    if length == BLOCK_SIZE
      length -= TRAILER_SIZE
    end

    if offset < DATA_OFFSET || offset > length
      raise "Invalid block data offset"
    end

    @buffer.slice(offset, length - offset)
  end

  # Return the log block trailer.
  def trailer
    @trailer ||= cursor(TRAILER_OFFSET).name("trailer") do |c|
      {
        :checksum => c.name("checksum") { c.get_uint32 },
      }
    end
  end

  # A helper function to return the checksum from the trailer, for
  # easier access.
  def checksum
    trailer[:checksum]
  end

  # Calculate the checksum of the block using InnoDB's log block
  # checksum algorithm.
  def calculate_checksum
    cksum = 1
    shift = (0..24).cycle
    cursor(0).each_byte_as_uint8(TRAILER_OFFSET) do |b|
      cksum &= 0x7fffffff
      cksum += b + (b << shift.next)
    end
    cksum
  end

  # Is the block corrupt? Calculate the checksum of the block and compare to
  # the stored checksum; return true or false.
  def corrupt?
    checksum != calculate_checksum
  end

  # Dump the contents of a log block for debugging purposes.
  def dump
    puts
    puts "header:"
    pp header

    puts
    puts "trailer:"
    pp trailer
  end
end
