# -*- encoding : utf-8 -*-
# An InnoDB transaction log file.
class Innodb::Log
  HEADER_SIZE   = 4 * Innodb::LogBlock::BLOCK_SIZE
  HEADER_START  = 0
  DATA_START    = HEADER_START + HEADER_SIZE

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
    @name = file
    File.open(@name) do |log|
      @size = log.stat.size
      @blocks = ((@size - DATA_START) / Innodb::LogBlock::BLOCK_SIZE)
    end
  end
  
  attr_reader :blocks

  # Return a log block with a given block number as an InnoDB::LogBlock object.
  # Blocks are numbered after the log file header, starting from 0.
  def block(block_number)
    offset = DATA_START + (block_number.to_i * Innodb::LogBlock::BLOCK_SIZE)
    return nil unless offset < @size
    return nil unless (offset + Innodb::LogBlock::BLOCK_SIZE) <= @size
    File.open(@name) do |log|
      log.seek(offset)
      block_data = log.read(Innodb::LogBlock::BLOCK_SIZE)
      Innodb::LogBlock.new(block_data)
    end
  end

  # Iterate through all log blocks, returning the block number and an
  # InnoDB::LogBlock object for each block.
  def each_block
    (0...@blocks).each do |block_number|
      current_block = block(block_number)
      yield block_number, current_block if current_block
    end
  end
end
