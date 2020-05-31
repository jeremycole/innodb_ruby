# frozen_string_literal: true

require 'forwardable'

# An InnoDB transaction log block.
module Innodb
  class LogBlock
    extend Forwardable

    Header = Struct.new(
      :flush,
      :block_number,
      :data_length,
      :first_rec_group,
      :checkpoint_no,
      keyword_init: true
    )

    Trailer = Struct.new(
      :checksum,
      keyword_init: true
    )

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
      raise "Log block buffer provided was not #{BLOCK_SIZE} bytes" unless buffer.size == BLOCK_SIZE

      @buffer = buffer
    end

    # Return an BufferCursor object positioned at a specific offset.
    def cursor(offset)
      BufferCursor.new(@buffer, offset)
    end

    # Return the log block header.
    def header
      @header ||= cursor(HEADER_OFFSET).name('header') do |c|
        Header.new(
          flush: c.name('flush') { c.peek { (c.read_uint32 & HEADER_FLUSH_BIT_MASK).positive? } },
          block_number: c.name('block_number') { c.read_uint32 & ~HEADER_FLUSH_BIT_MASK },
          data_length: c.name('data_length') { c.read_uint16 },
          first_rec_group: c.name('first_rec_group') { c.read_uint16 },
          checkpoint_no: c.name('checkpoint_no') { c.read_uint32 }
        )
      end
    end

    def_delegator :header, :flush
    def_delegator :header, :block_number
    def_delegator :header, :data_length
    def_delegator :header, :first_rec_group
    def_delegator :header, :checkpoint_no

    # Return a slice of actual block data (that is, excluding header and
    # trailer) starting at the given offset.
    def data(offset = DATA_OFFSET)
      length = data_length
      length -= TRAILER_SIZE if length == BLOCK_SIZE

      raise 'Invalid block data offset' if offset < DATA_OFFSET || offset > length

      @buffer.slice(offset, length - offset)
    end

    # Return the log block trailer.
    def trailer
      @trailer ||= cursor(TRAILER_OFFSET).name('trailer') do |c|
        Trailer.new(checksum: c.name('checksum') { c.read_uint32 })
      end
    end

    def_delegator :trailer, :checksum

    # Calculate the checksum of the block using InnoDB's log block
    # checksum algorithm.
    def calculate_checksum
      csum = 1
      shift = (0..24).cycle
      cursor(0).each_byte_as_uint8(TRAILER_OFFSET) do |b|
        csum &= 0x7fffffff
        csum += b + (b << shift.next)
      end
      csum
    end

    # Is the block corrupt? Calculate the checksum of the block and compare to
    # the stored checksum; return true or false.
    def corrupt?
      checksum != calculate_checksum
    end

    # Dump the contents of a log block for debugging purposes.
    def dump
      puts
      puts 'header:'
      pp header

      puts
      puts 'trailer:'
      pp trailer
    end
  end
end
