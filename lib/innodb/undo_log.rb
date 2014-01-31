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

  def read_header(cursor)
    cursor.name("header") do |c|
      xid_exists = nil
      {
        :trx_id => c.name("trx_id") { c.get_hex(8) },
        :trx_no => c.name("trx_no") { c.get_uint64 },
        :delete_marks => c.name("delete_marks") { (c.get_uint16 != 0) },
        :log_start => c.name("log_start") { c.get_uint16 },
        :xid_exists => c.name("xid_exists") { xid_exists = (c.get_uint8 != 0) },
        :dict_trans => c.name("dict_trans") { (c.get_uint8 != 0) },
        :table_id => c.name("table_id") { c.get_uint64 },
        :next_log => c.name("next_log") { c.get_uint16 },
        :prev_log => c.name("prev_log") { c.get_uint16 },
        :history_list_node => c.name("history_list_node") {
          Innodb::List.get_node(c)
        },
        :xid => c.name("xid") {
          if xid_exists
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

  def undo_log
    @undo_log ||= page.cursor(position).name("undo_log") do |c|
      {
        :header => read_header(c),
      }
    end
  end

  def header
    undo_log[:header]
  end

  def prev_address
    header[:history_list_node][:prev]
  end

  def next_address
    header[:history_list_node][:next]
  end

  def dump
    puts "header:"
    pp header
    puts
  end
end
