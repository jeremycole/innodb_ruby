# -*- encoding : utf-8 -*-

# Group of InnoDB logs files that make up the redo log.
class Innodb::LogGroup

  # Initialize group given a set of sorted log files.
  def initialize(log_files)
    @logs = log_files.map { |fn| Innodb::Log.new(fn) }
    sizes = @logs.map { |log| log.size }
    raise "Log file sizes do not match" unless sizes.uniq.size == 1
  end

  # Iterate through all logs.
  def each_log
    unless block_given?
      return enum_for(:each_log)
    end

    @logs.each do |log|
      yield log
    end
  end

  # Iterate through all blocks.
  def each_block
    unless block_given?
      return enum_for(:each_block)
    end

    each_log do |log|
      log.each_block do |block_index, block|
        yield block_index, block
      end
    end
  end
end
