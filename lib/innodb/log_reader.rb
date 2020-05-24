# frozen_string_literal: true

# Representation of the log group as a seekable stream of log records.
module Innodb
  class LogReader
    # Checksum failed exception.
    class ChecksumError < RuntimeError; end

    # EOF reached exception.
    class EOFError < EOFError; end

    Context = Struct.new(
      :buffer,
      :buffer_lsn,
      :record_lsn,
      keyword_init: true
    )

    # Whether to checksum blocks.
    # TODO: Hmm, nothing seems to actually set this.
    attr_accessor :checksum

    def initialize(lsn, group)
      @group = group
      @context = Context.new(buffer: String.new, buffer_lsn: lsn.dup, record_lsn: lsn.dup)
    end

    # Seek to record starting position.
    def seek(lsn_no)
      check_lsn_no(lsn_no)
      @context.buffer = String.new
      @context.buffer_lsn.reposition(lsn_no, @group)
      @context.record_lsn = @context.buffer_lsn.dup
      self
    end

    # Returns the current LSN starting position.
    def tell
      @context.record_lsn.no
    end

    # Read a record.
    def record
      cursor = BufferCursor.new(self, 0)
      record = Innodb::LogRecord.new
      record.read(cursor)
      record.lsn = reposition(cursor.position)
      record
    end

    # Call the given block once for each record in the log until the
    # end of the log (or a corrupted block) is reached. If the follow
    # argument is true, retry.
    def each_record(follow, wait = 0.5)
      loop { yield record }
    rescue EOFError, ChecksumError
      sleep(wait) && retry if follow
    end

    # Read a slice of log data (that is, log data used for records).
    def slice(position, length)
      buffer = @context.buffer
      length = position + length

      preload(length) if length > buffer.size

      buffer.slice(position, length - position)
    end

    private

    # Check if LSN points to where records may be located.
    def check_lsn_no(lsn_no)
      lsn = @context.record_lsn.dup
      lsn.reposition(lsn_no, @group)
      raise "LSN #{lsn_no} is out of bounds" unless lsn.record?(@group)
    end

    # Reposition to the beginning of the next record.
    def reposition(length)
      start_lsn_no = @context.record_lsn.no
      delta_lsn_no = @context.record_lsn.delta(length)
      @context.record_lsn.advance(delta_lsn_no, @group)
      @context.buffer.slice!(0, length)
      [start_lsn_no, start_lsn_no + delta_lsn_no]
    end

    # Reads the log block at the given LSN position.
    def get_block(lsn)
      log_no, block_no, block_offset = lsn.location(@group)
      [@group.log(log_no).block(block_no), block_offset]
    end

    # Preload the log buffer with enough data to satisfy the requested amount.
    def preload(size)
      buffer = @context.buffer
      buffer_lsn = @context.buffer_lsn

      # If reading for the first time, offset points to the start of the
      # record (somewhere in the block). Otherwise, the block is read as
      # a whole and offset points to the start of the next block to read.
      while buffer.size < size
        block, offset = get_block(buffer_lsn)
        break if checksum && (corrupt = block.corrupt?)

        data = offset.zero? ? block.data : block.data(offset)
        data_length = block.header[:data_length]
        buffer << data
        buffer_lsn.advance(data_length - offset, @group)
        break if data_length < Innodb::LogBlock::BLOCK_SIZE
      end

      raise ChecksumError, 'Block is corrupted' if corrupt
      raise EOFError, 'End of log reached' if buffer.size < size
    end
  end
end
