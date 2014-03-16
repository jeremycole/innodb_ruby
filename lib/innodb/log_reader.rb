# -*- encoding : utf-8 -*-

require "ostruct"

# Representation of the log group as a seekable stream of log records.
class Innodb::LogReader

  def initialize(lsn, group)
    @group = group
    @context = OpenStruct.new(:buffer => String.new,
      :buffer_lsn => lsn.dup, :record_lsn => lsn.dup)
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

  # Read a slice of log data (that is, log data used for records).
  def slice(position, length)
    buffer = @context.buffer
    length = position + length

    if length > buffer.size
      preload(length)
    end

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
      data = offset == 0 ? block.data : block.data(offset)
      data_length = block.header[:data_length]
      buffer << data
      buffer_lsn.advance(data_length - offset, @group)
      break if data_length < Innodb::LogBlock::BLOCK_SIZE
    end

    raise EOFError, "End of log reached" if buffer.size < size
  end
end
