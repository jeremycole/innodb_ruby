# -*- encoding : utf-8 -*-

# A single history list; this is a more intelligent wrapper around the basic
# Innodb::List::History which is provided elsewhere.
class Innodb::HistoryList
  attr_reader :list

  # Initialize from a provided Innodb::List::History.
  def initialize(list)
    @list = list
  end

  class UndoRecordCursor
    def initialize(history, undo_record, direction=:forward)
      @history = history
      @undo_record = undo_record

      case undo_record
      when :min
        @undo_log_cursor = history.list.list_cursor(:min, direction)
        if @undo_log = @undo_log_cursor.node
          @undo_record_cursor = @undo_log.undo_record_cursor(:min, direction)
        end
      when :max
        @undo_log_cursor = history.list.list_cursor(:max, direction)
        if @undo_log = @undo_log_cursor.node
          @undo_record_cursor = @undo_log.undo_record_cursor(:max, direction)
        end
      else
        raise "Not implemented"
      end
    end

    def undo_record
      unless @undo_record_cursor
        return nil
      end

      if rec = @undo_record_cursor.undo_record
        return rec
      end

      case @direction
      when :forward
        next_undo_record
      when :backward
        prev_undo_record
      end
    end

    def move_cursor(page, undo_record)
      @undo_log = page
      @undo_log_cursor = @undo_log.undo_record_cursor(undo_record, @direction)
    end

    def next_undo_record
      if rec = @undo_record_cursor.undo_record
        return rec
      end

      if undo_log = @undo_log_cursor.node
        @undo_log = undo_log
        @undo_record_cursor = @undo_log.undo_record_cursor(:min, @direction)
      end

      @undo_record_cursor.undo_record
    end

    def prev_undo_record
      if rec = @undo_log_cursor.undo_record
        return rec
      end

      if undo_log = @undo_log_cursor.node
        @undo_log = undo_log
        @undo_record_cursor = @undo_log.undo_record_cursor(:max, @direction)
      end

      @undo_record_cursor.undo_record
    end

    def each_undo_record
      unless block_given?
        return enum_for(:each_undo_record)
      end

      while rec = undo_record
        yield rec
      end
    end
  end

  def undo_record_cursor(undo_record=:min, direction=:forward)
    UndoRecordCursor.new(self, undo_record, direction)
  end

  def each_undo_record
    unless block_given?
      return enum_for(:each_undo_record)
    end

    undo_record_cursor.each_undo_record do |rec|
      yield rec
    end
  end
end
