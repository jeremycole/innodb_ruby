# frozen_string_literal: true

# Group of InnoDB logs files that make up the redo log.
module Innodb
  class LogGroup
    # Initialize group given a set of sorted log files.
    def initialize(log_files)
      @logs = log_files.map { |fn| Innodb::Log.new(fn) }
      raise 'Log file sizes do not match' unless @logs.map(&:size).uniq.size == 1
    end

    # Iterate through all logs.
    def each_log
      return enum_for(:each_log) unless block_given?

      @logs.each do |log|
        yield log
      end
    end

    # Iterate through all blocks.
    def each_block
      return enum_for(:each_block) unless block_given?

      each_log do |log|
        log.each_block do |block_index, block|
          yield block_index, block
        end
      end
    end

    # The number of log files in the group.
    def logs
      @logs.count
    end

    # Returns the log at the given position in the log group.
    def log(log_no)
      @logs.at(log_no)
    end

    # The size in byes of each and every log in the group.
    def log_size
      @logs.first.size
    end

    # The size of the log group (in bytes)
    def size
      @logs.first.size * @logs.count
    end

    # The log group capacity (in bytes).
    def capacity
      @logs.first.capacity * @logs.count
    end

    # Returns the LSN coordinates of the data at the start of the log group.
    def start_lsn
      [@logs.first.header[:start_lsn], Innodb::Log::LOG_HEADER_SIZE]
    end

    # Returns the LSN coordinates of the most recent (highest) checkpoint.
    def max_checkpoint_lsn
      @logs.first.checkpoint.max_by(&:number).to_h.values_at(:lsn, :lsn_offset)
    end

    # Returns a LogReader using the given LSN reference coordinates.
    def reader(lsn_coordinates = start_lsn)
      Innodb::LogReader.new(Innodb::LSN.new(*lsn_coordinates), self)
    end

    # Parse and return a record at a given LSN.
    def record(lsn_no)
      reader.seek(lsn_no).record
    end
  end
end
