# -*- encoding : utf-8 -*-

# An InnoDB transaction log file.
class Innodb::Log
  # A map of the name and position of the blocks that form the log header.
  LOG_HEADER_BLOCK_MAP = {
    :LOG_FILE_HEADER  => 0,
    :LOG_CHECKPOINT_1 => 1,
    :EMPTY            => 2,
    :LOG_CHECKPOINT_2 => 3,
  }

  # Number of blocks in the log file header.
  LOG_HEADER_BLOCKS = LOG_HEADER_BLOCK_MAP.size

  # The size in bytes of the log file header.
  LOG_HEADER_SIZE = LOG_HEADER_BLOCKS * Innodb::LogBlock::BLOCK_SIZE

  # Maximum number of log group checkpoints.
  LOG_CHECKPOINT_GROUPS = 32

  # Open a log file.
  def initialize(filename)
    @file = File.open(filename)
    @size = @file.stat.size
    @blocks = (@size / Innodb::LogBlock::BLOCK_SIZE) - LOG_HEADER_BLOCKS
    @capacity = @blocks * Innodb::LogBlock::BLOCK_SIZE
  end

  # The size (in bytes) of the log.
  attr_reader :size

  # The log capacity (in bytes).
  attr_reader :capacity

  # The number of blocks in the log.
  attr_reader :blocks

  # Get the raw byte buffer for a specific block by block offset.
  def block_data(offset)
    raise "Invalid block offset" unless (offset % Innodb::LogBlock::BLOCK_SIZE).zero?
    @file.sysseek(offset)
    @file.sysread(Innodb::LogBlock::BLOCK_SIZE)
  end

  # Get a cursor to a block in a given offset of the log.
  def block_cursor(offset)
    BufferCursor.new(block_data(offset), 0)
  end

  # Return the log header.
  def header
    offset = LOG_HEADER_BLOCK_MAP[:LOG_FILE_HEADER] * Innodb::LogBlock::BLOCK_SIZE
    @header ||= block_cursor(offset).name("header") do |c|
      {
        :group_id   => c.name("group_id")   { c.get_uint32 },
        :start_lsn  => c.name("start_lsn")  { c.get_uint64 },
        :file_no    => c.name("file_no")    { c.get_uint32 },
        :created_by => c.name("created_by") { c.get_string(32) }
      }
    end
  end

  # Read a log checkpoint from the given cursor.
  def read_checkpoint(c)
    # Log archive related fields (e.g. group_array) are not currently in
    # use or even read by InnoDB. However, for the sake of completeness,
    # they are included.
    {
      :number         => c.name("number")       { c.get_uint64 },
      :lsn            => c.name("lsn")          { c.get_uint64 },
      :lsn_offset     => c.name("lsn_offset")   { c.get_uint32 },
      :buffer_size    => c.name("buffer_size")  { c.get_uint32 },
      :archived_lsn   => c.name("archived_lsn") { c.get_uint64 },
      :group_array    =>
        (0 .. LOG_CHECKPOINT_GROUPS - 1).map do |n|
          c.name("group_array[#{n}]") do
            {
              :archived_file_no => c.name("archived_file_no") { c.get_uint32 },
              :archived_offset  => c.name("archived_offset")  { c.get_uint32 },
            }
          end
        end,
      :checksum_1     => c.name("checksum_1")     { c.get_uint32 },
      :checksum_2     => c.name("checksum_2")     { c.get_uint32 },
      :fsp_free_limit => c.name("fsp_free_limit") { c.get_uint32 },
      :fsp_magic      => c.name("fsp_magic")      { c.get_uint32 },
    }
  end

  # Return the log checkpoints.
  def checkpoint
    offset1 = LOG_HEADER_BLOCK_MAP[:LOG_CHECKPOINT_1] * Innodb::LogBlock::BLOCK_SIZE
    offset2 = LOG_HEADER_BLOCK_MAP[:LOG_CHECKPOINT_2] * Innodb::LogBlock::BLOCK_SIZE
    @checkpoint ||=
      {
        :checkpoint_1 => block_cursor(offset1).name("checkpoint_1") do |cursor|
          cp = read_checkpoint(cursor)
          cp.delete(:group_array)
          cp
        end,
        :checkpoint_2 => block_cursor(offset2).name("checkpoint_2") do |cursor|
          cp = read_checkpoint(cursor)
          cp.delete(:group_array)
          cp
        end
      }
  end

  # Return a log block with a given block index as an InnoDB::LogBlock object.
  # Blocks are indexed after the log file header, starting from 0.
  def block(block_index)
    return nil unless block_index.between?(0, @blocks - 1)
    offset = (LOG_HEADER_BLOCKS + block_index.to_i) * Innodb::LogBlock::BLOCK_SIZE
    Innodb::LogBlock.new(block_data(offset))
  end

  # Iterate through all log blocks, returning the block index and an
  # InnoDB::LogBlock object for each block.
  def each_block
    (0...@blocks).each do |block_index|
      current_block = block(block_index)
      yield block_index, current_block if current_block
    end
  end
end
