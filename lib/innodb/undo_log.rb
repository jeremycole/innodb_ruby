# -*- encoding : utf-8 -*-

class Innodb::UndoLog
  attr_reader :page
  attr_reader :position
  def initialize(page, position)
    @page = page
    @position = position
  end

  def size_xa_header
    4 + 4 + 4 + 128
  end

  def size_header
    8 + 8 + 2 + 2 + 1 + 1 + 8 + 2 + 2 + Innodb::List::NODE_SIZE + size_xa_header
  end

  def header
    @header ||= page.cursor(@position).name("header") do |c|
      xid_flag = nil
      {
        :trx_id => c.name("trx_id") { c.get_uint64 },
        :trx_no => c.name("trx_no") { c.get_uint64 },
        :delete_mark_flag => c.name("delete_mark_flag") { (c.get_uint16 != 0) },
        :log_start_offset => c.name("log_start_offset") { c.get_uint16 },
        :xid_flag => c.name("xid_flag") { xid_flag = (c.get_uint8 != 0) },
        :ddl_flag => c.name("ddl_flag") { (c.get_uint8 != 0) },
        :ddl_table_id => c.name("ddl_table_id") { c.get_uint64 },
        :next_log_offset => c.name("next_log_offset") { c.get_uint16 },
        :prev_log_offset => c.name("prev_log_offset") { c.get_uint16 },
        :history_list_node => c.name("history_list_node") {
          Innodb::List.get_node(c)
        },
        :xid => c.name("xid") {
          if xid_flag
            {
              :format => c.name("format") { c.get_uint32 },
              :trid_len => c.name("trid_len") { c.get_uint32 },
              :bqual_len => c.name("bqual_len") { c.get_uint32 },
              :data => c.name("data") { c.get_bytes(128) },
            }
          end
        },
      }
    end
  end

  def prev_address
    header[:history_list_node][:prev]
  end

  def next_address
    header[:history_list_node][:next]
  end

  def undo_record(offset)
    new_undo_record = Innodb::UndoRecord.new(page, offset)
    new_undo_record.undo_log = self
    new_undo_record
  end

  def min_undo_record
    undo_record(header[:log_start_offset])
  end

  class UndoRecordCursor
    def initialize(undo_log, offset, direction=:forward)
      @initial = true
      @undo_log = undo_log
      @offset = offset
      @direction = direction

      case offset
      when :min
        @undo_record = @undo_log.min_undo_record
      when :max
        raise "Not implemented"
      else
        @undo_record = @undo_log.undo_record(offset)
      end
    end

    def next_undo_record
      if rec = @undo_record.next
        @undo_record = rec
      end
    end

    def prev_undo_record
      if rec = @undo_record.prev
        @undo_record = rec
      end
    end

    def undo_record
      if @initial
        @initial = false
        return @undo_record
      end

      case @direction
      when :forward
        next_undo_record
      when :backward
        prev_undo_record
      end
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

  def undo_record_cursor(offset, direction=:forward)
    UndoRecordCursor.new(self, offset, direction)
  end

  def first_undo_record_cursor
    undo_record_cursor(header[:log_start_offset])
  end

  def dump
    puts "header:"
    pp header
    puts
  end
end
