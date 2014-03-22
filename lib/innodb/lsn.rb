# -*- encoding : utf-8 -*-

# A Log Sequence Number and its byte offset into the log group.
class Innodb::LSN
  # The Log Sequence Number.
  attr_reader :lsn_no

  # Alias :lsn_no attribute.
  alias_method :no, :lsn_no

  # Initialize coordinates.
  def initialize(lsn, offset)
    @lsn_no = lsn
    @lsn_offset = offset
  end

  # Place LSN in a new position.
  def reposition(new_lsn_no, group)
    new_offset = offset_of(@lsn_no, @lsn_offset, new_lsn_no, group)
    @lsn_no, @lsn_offset = [new_lsn_no, new_offset]
  end

  # Advance by a given LSN amount.
  def advance(count_lsn_no, group)
    new_lsn_no = @lsn_no + count_lsn_no
    reposition(new_lsn_no, group)
  end

  # Returns the location coordinates of this LSN.
  def location(group)
    location_of(@lsn_offset, group)
  end

  # Returns the LSN delta for the given amount of data.
  def delta(length)
    fragment = (@lsn_no % LOG_BLOCK_SIZE) - LOG_BLOCK_HEADER_SIZE
    raise "Invalid fragment #{fragment} for LSN #{@lsn_no}" unless
      fragment.between?(0, LOG_BLOCK_DATA_SIZE - 1)
    length + (fragment + length) / LOG_BLOCK_DATA_SIZE * LOG_BLOCK_FRAME_SIZE
  end

  # Whether LSN might point to log record data.
  def record?(group)
    data_offset?(@lsn_offset, group)
  end

  private

  # Short alias for the size of a log file header.
  LOG_HEADER_SIZE = Innodb::Log::LOG_HEADER_SIZE

  # Short aliases for the sizes of the subparts of a log block.
  LOG_BLOCK_SIZE = Innodb::LogBlock::BLOCK_SIZE
  LOG_BLOCK_HEADER_SIZE = Innodb::LogBlock::HEADER_SIZE
  LOG_BLOCK_TRAILER_SIZE = Innodb::LogBlock::TRAILER_SIZE
  LOG_BLOCK_DATA_SIZE = Innodb::LogBlock::DATA_SIZE
  LOG_BLOCK_FRAME_SIZE = LOG_BLOCK_HEADER_SIZE + LOG_BLOCK_TRAILER_SIZE

  # Returns the coordinates of the given offset.
  def location_of(offset, group)
    log_no, log_offset = offset.divmod(group.size)
    block_no, block_offset = (log_offset - LOG_HEADER_SIZE).divmod(LOG_BLOCK_SIZE)
    [log_no, block_no, block_offset]
  end

  # Returns the offset of the given LSN within a log group.
  def offset_of(lsn, offset, new_lsn, group)
    log_size = group.log_size
    group_capacity = group.capacity

    # Calculate the offset in LSN.
    if new_lsn >= lsn
      lsn_offset = new_lsn - lsn
    else
      lsn_offset = lsn - new_lsn
      lsn_offset %= group_capacity
      lsn_offset = group_capacity - lsn_offset
    end

    # Transpose group size offset to a group capacity offset.
    group_offset = offset - (LOG_HEADER_SIZE * (1 + offset / log_size))

    offset = (lsn_offset + group_offset) % group_capacity

    # Transpose group capacity offset to a group size offset.
    offset + LOG_HEADER_SIZE * (1 + offset / (log_size - LOG_HEADER_SIZE))
  end

  # Whether offset points to the data area of an existing log block.
  def data_offset?(offset, group)
    log_offset = offset % group.size
    log_no, block_no, block_offset = location_of(offset, group)

    status ||= log_no > group.logs
    status ||= log_offset <= LOG_HEADER_SIZE
    status ||= block_no < 0
    status ||= block_no >= group.log(log_no).blocks
    status ||= block_offset < Innodb::LogBlock::DATA_OFFSET
    status ||= block_offset >= Innodb::LogBlock::TRAILER_OFFSET

    !status
  end
end
