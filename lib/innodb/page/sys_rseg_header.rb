# -*- encoding : utf-8 -*-

class Innodb::Page::SysRsegHeader < Innodb::Page
  # The number of undo log slots in the page.
  UNDO_SEGMENT_SLOTS = 1024

  # The position of the rollback segment header within the page.
  def pos_rseg_header
    pos_page_body
  end

  # The size of the rollback segment header.
  def size_rseg_header
    4 + 4 + Innodb::List::BASE_NODE_SIZE + Innodb::FsegEntry::SIZE
  end

  def pos_undo_segment_array
    pos_rseg_header + size_rseg_header
  end

  def size_undo_segment_slot
    4
  end

  # Parse the rollback segment header from the page.
  def rseg_header
    cursor(pos_rseg_header).name("rseg_header") do |c|
      {
        :max_size => c.name("max_size") { c.get_uint32 },
        :history_size => c.name("history_size") { c.get_uint32 },
        :history_list => c.name("history_list") {
          Innodb::List::History.new(@space, Innodb::List.get_base_node(c))
        },
        :fseg => c.name("fseg") { Innodb::FsegEntry.get_inode(@space, c) },
      }
    end
  end

  def history_list
    Innodb::HistoryList.new(rseg_header[:history_list])
  end

  def each_undo_segment
    unless block_given?
      return enum_for(:each_undo_segment)
    end

    cursor(pos_undo_segment_array).name("undo_segment_array") do |c|
      (0...UNDO_SEGMENT_SLOTS).each do |slot|
        page_number = c.name("slot[#{slot}]") {
          Innodb::Page.maybe_undefined(c.get_uint32)
        }
        yield slot, page_number
      end
    end
  end

  def each_region
    unless block_given?
      return enum_for(:each_region)
    end

    super do |region|
      yield region
    end

    yield({
      :offset => pos_rseg_header,
      :length => size_rseg_header,
      :name => :rseg_header,
      :info => "Rollback Segment Header",
    })

    (0...UNDO_SEGMENT_SLOTS).each do |slot|
      yield({
        :offset => pos_undo_segment_array + (slot * size_undo_segment_slot),
        :length => size_undo_segment_slot,
        :name => :undo_segment_slot,
        :info => "Undo Segment Slot",
      })
    end

    nil
  end

  def dump
    super

    puts
    puts "rollback segment header:"
    pp rseg_header

    puts
    puts "undo segment array:"
    each_undo_segment do |slot, page_number|
      puts "  #{slot}: #{page_number}"
    end
  end
end
