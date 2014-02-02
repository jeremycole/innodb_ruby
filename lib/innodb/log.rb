# -*- encoding : utf-8 -*-

# An InnoDB transaction log file.

class Innodb::Log
  # Number of blocks in the log file header.
  LOG_HEADER_BLOCKS = 4

#define LOG_GROUP_ID		0	/* log group number */
#define LOG_FILE_START_LSN	4	/* lsn of the start of data in this
#define LOG_FILE_NO		12	/* 4-byte archived log file number;
#define LOG_FILE_WAS_CREATED_BY_HOT_BACKUP 16
#define	LOG_FILE_ARCH_COMPLETED	OS_FILE_LOG_BLOCK_SIZE
#define LOG_FILE_END_LSN	(OS_FILE_LOG_BLOCK_SIZE + 4)
#define LOG_CHECKPOINT_1	OS_FILE_LOG_BLOCK_SIZE
#define LOG_CHECKPOINT_2	(3 * OS_FILE_LOG_BLOCK_SIZE)
#define LOG_FILE_HDR_SIZE	(4 * OS_FILE_LOG_BLOCK_SIZE)

  # Open a log file.
  def initialize(file)
    @file = File.open(file)
    @size = @file.stat.size
    @blocks = (@size / Innodb::LogBlock::BLOCK_SIZE) - LOG_HEADER_BLOCKS
  end

  # The size (in bytes) of the log.
  attr_reader :size

  # The number of blocks in the the log.
  attr_reader :blocks

  # Get the raw byte buffer for a specific block by block offset.
  def block_data(offset)
    raise "Invalid block offset" unless (offset % Innodb::LogBlock::BLOCK_SIZE).zero?
    @file.seek(offset)
    @file.read(Innodb::LogBlock::BLOCK_SIZE)
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
