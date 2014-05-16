# -*- encoding : utf-8 -*-

class Innodb::Page::UndoLog < Innodb::Page
  def pos_undo_page_header
    pos_page_body
  end

  def size_undo_page_header
    2 + 2 + 2 + Innodb::List::NODE_SIZE
  end

  def pos_undo_segment_header
    pos_undo_page_header + size_undo_page_header
  end

  def size_undo_segment_header
    2 + 2 + Innodb::FsegEntry::SIZE + Innodb::List::BASE_NODE_SIZE
  end

  def pos_undo_logs
    pos_undo_segment_header + size_undo_segment_header
  end

  UNDO_PAGE_TYPES = {
    1 => :insert,
    2 => :update,
  }

  UNDO_SEGMENT_STATES = {
    1 => :active,
    2 => :cached,
    3 => :to_free,
    4 => :to_purge,
    5 => :prepared,
  }

  def undo_page_header
    @undo_page_header ||=
      cursor(pos_undo_page_header).name("undo_page_header") do |c|
      {
        :type => c.name("type") { UNDO_PAGE_TYPES[c.get_uint16] },
        :latest_log_record_offset => c.name("latest_log_record_offset") { c.get_uint16 },
        :free_offset => c.name("free_offset") { c.get_uint16 },
        :page_list_node => c.name("page_list") { Innodb::List.get_node(c) },
      }
    end
  end

  def prev_address
    undo_page_header[:page_list_node][:prev]
  end

  def next_address
    undo_page_header[:page_list_node][:next]
  end

  def undo_segment_header
    @undo_segment_header ||=
      cursor(pos_undo_segment_header).name("undo_segment_header") do |c|
      {
        :state => c.name("state") { UNDO_SEGMENT_STATES[c.get_uint16] },
        :last_log_offset => c.name("last_log_offset") { c.get_uint16 },
        :fseg => c.name("fseg") { Innodb::FsegEntry.get_inode(@space, c) },
        :page_list => c.name("page_list") {
          Innodb::List::UndoPage.new(@space, Innodb::List.get_base_node(c))
        },
      }
    end
  end

  def undo_log(pos)
    Innodb::UndoLog.new(self, pos)
  end

  # Dump the contents of a page for debugging purposes.
  def dump
    super

    puts "undo page header:"
    pp undo_page_header
    puts

    puts "undo segment header:"
    pp undo_segment_header
    puts

    puts "last undo log:"
    if undo_segment_header[:last_log_offset] != 0
      undo_log(undo_segment_header[:last_log_offset]).dump
    end
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:UNDO_LOG] = Innodb::Page::UndoLog
