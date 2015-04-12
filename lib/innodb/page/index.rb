# -*- encoding : utf-8 -*-

require "innodb/fseg_entry"

# A class for handling common structure that underlies both uncompressed
# and uncompressed INDEX pages.
#
# The basic structure of an INDEX page is: FIL header, INDEX header, FSEG
# header, and various fields specific to uncompressed/compressed pages.
class Innodb::Page::Index < Innodb::Page
  # Maximum number of fields.
  RECORD_MAX_N_SYSTEM_FIELDS  = 3
  RECORD_MAX_N_FIELDS         = 1024 - 1
  RECORD_MAX_N_USER_FIELDS    = RECORD_MAX_N_FIELDS - RECORD_MAX_N_SYSTEM_FIELDS * 2

  # Length of InnoDB system columns.
  SYS_FIELD_ROW_ID_LENGTH     = 6
  SYS_FIELD_TRX_ID_LENGTH     = 6
  SYS_FIELD_ROLL_PTR_LENGTH   = 7
  SYS_FIELD_NODE_PTR_LENGTH   = 4

  # Page direction values possible in the page_header's :direction field.
  PAGE_DIRECTION = {
    1 => :left,           # Inserts have been in descending order.
    2 => :right,          # Inserts have been in ascending order.
    3 => :same_rec,       # Unused by InnoDB.
    4 => :same_page,      # Unused by InnoDB.
    5 => :no_direction,   # Inserts have been in random order.
  }

  def self.handle(page, space, buffer)
    if space.compressed
      Innodb::Page::Index::Compressed.new(space, buffer)
    else
      Innodb::Page::Index::Uncompressed.new(space, buffer)
    end
  end

  # Return the byte offset of the start of the "index" page header, which
  # immediately follows the "fil" header.
  def pos_index_header
    pos_page_body
  end

  # The size of the "index" header.
  def size_index_header
    2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 8 + 2 + 8
  end

  # Return the "index" header.
  def page_header
    @page_header ||= cursor(pos_index_header).name("index") do |c|
      index = {
        :n_dir_slots            => c.name("n_dir_slots") { c.get_uint16 },
        :heap_top               => c.name("heap_top") { c.get_uint16 },
        :n_heap_format          => c.name("n_heap_format") { c.get_uint16 },
        :garbage_offset         => c.name("garbage_offset") { c.get_uint16 },
        :garbage_size           => c.name("garbage_size") { c.get_uint16 },
        :last_insert_offset     => c.name("last_insert_offset") { c.get_uint16 },
        :direction              => c.name("direction") { PAGE_DIRECTION[c.get_uint16] },
        :n_direction            => c.name("n_direction") { c.get_uint16 },
        :n_recs                 => c.name("n_recs") { c.get_uint16 },
        :max_trx_id             => c.name("max_trx_id") { c.get_uint64 },
        :level                  => c.name("level") { c.get_uint16 },
        :index_id               => c.name("index_id") { c.get_uint64 },
      }
      index[:n_heap] = index[:n_heap_format] & (2**15-1)
      index[:format] = (index[:n_heap_format] & 1<<15) == 0 ?
        :redundant : :compact
      index.delete :n_heap_format

      index
    end
  end

  # A helper function to return the index id.
  def index_id
    page_header && page_header[:index_id]
  end

  # A helper function to return the page level from the "page" header, for
  # easier access.
  def level
    page_header && page_header[:level]
  end

  # A helper function to return the number of records.
  def records
    page_header && page_header[:n_recs]
  end

  # A helper function to identify root index pages; they must be the only
  # pages at their level.
  def root?
    self.prev.nil? && self.next.nil?
  end

  # A helper function to identify leaf index pages.
  def leaf?
    level == 0
  end

  # Return the byte offset of the start of the "fseg" header, which immediately
  # follows the "index" header.
  def pos_fseg_header
    pos_index_header + size_index_header
  end

  # The size of the "fseg" header.
  def size_fseg_header
    2 * Innodb::FsegEntry::SIZE
  end

  # Return the "fseg" header.
  def fseg_header
    @fseg_header ||= cursor(pos_fseg_header).name("fseg") do |c|
      {
        :leaf     => c.name("fseg[leaf]") {
          Innodb::FsegEntry.get_inode(@space, c)
        },
        :internal => c.name("fseg[internal]") {
          Innodb::FsegEntry.get_inode(@space, c)
        },
      }
    end
  end

  # The amount of space consumed by the page header.
  def header_space
    size_fil_header +
      size_index_header +
      size_fseg_header
  end

  def record_describer=(o)
    @record_describer = o
  end

  def record_describer
    return @record_describer if @record_describer

    if space and space.innodb_system and index_id
      @record_describer =
        space.innodb_system.data_dictionary.record_describer_by_index_id(index_id)
    elsif space
      @record_describer = space.record_describer
    end

    @record_describer
  end

  # Return a set of field objects that describe the record.
  def make_record_description
    position = (0..RECORD_MAX_N_FIELDS).each
    description = record_describer.description
    fields = {:type => description[:type], :key => [], :sys => [], :row => []}

    description[:key].each do |field|
      fields[:key] << Innodb::Field.new(position.next, field[:name], *field[:type])
    end

    # If this is a leaf page of the clustered index, read InnoDB's internal
    # fields, a transaction ID and roll pointer.
    if level == 0 && fields[:type] == :clustered
      [["DB_TRX_ID", :TRX_ID,],["DB_ROLL_PTR", :ROLL_PTR]].each do |name, type|
        fields[:sys] << Innodb::Field.new(position.next, name, type, :NOT_NULL)
      end
    end

    # If this is a leaf page of the clustered index, or any page of a
    # secondary index, read the non-key fields.
    if (level == 0 && fields[:type] == :clustered) || (fields[:type] == :secondary)
      description[:row].each do |field|
        fields[:row] << Innodb::Field.new(position.next, field[:name], *field[:type])
      end
    end

    fields
  end

  # Return (and cache) the record format provided by an external class.
  def record_format
    if record_describer
      @record_format ||= make_record_description()
    end
  end

  # Returns the (ordered) set of fields that describe records in this page.
  def record_fields
    if record_format
      record_format.values_at(:key, :sys, :row).flatten.sort_by {|f| f.position}
    end
  end

  # A class for cursoring through records starting from an arbitrary point.
  class RecordCursor
    def initialize(page, offset, direction)
      Innodb::Stats.increment :page_record_cursor_create

      @initial = true
      @page = page
      @direction = direction
      case offset
      when :min
        @record = @page.min_record
      when :max
        @record = @page.max_record
      else
        # Offset is a byte offset of a record (hopefully).
        @record = @page.record(offset)
      end
    end

    # Return the next record, and advance the cursor. Return nil when the
    # end of records (supremum) is reached.
    def next_record
      Innodb::Stats.increment :page_record_cursor_next_record

      rec = @page.record(@record.next)

      # The garbage record list's end is self-linked, so we must check for
      # both supremum and the current record's offset.
      if rec == @page.supremum || rec.offset == @record.offset
        # We've reached the end of the linked list at supremum.
        nil
      else
        @record = rec
      end
    end

    # Return the previous record, and advance the cursor. Return nil when the
    # end of records (infimum) is reached.
    def prev_record
      Innodb::Stats.increment :page_record_cursor_prev_record

      unless slot = @page.directory_slot_for_record(@record)
        raise "Couldn't find slot for record"
      end

      unless search_cursor = @page.record_cursor(@page.directory[slot-1])
        raise "Couldn't position search cursor"
      end

      while rec = search_cursor.record and rec.offset != @record.offset
        if rec.next == @record.offset
          if rec == @page.infimum
            return nil
          end
          return @record = rec
        end
      end
    end

    # Return the next record in the order defined when the cursor was created.
    def record
      if @initial
        @initial = false
        return @record
      end

      case @direction
      when :forward
        next_record
      when :backward
        prev_record
      end
    end

    # Iterate through all records in the cursor.
    def each_record
      unless block_given?
        return enum_for(:each_record)
      end

      while rec = record
        yield rec
      end
    end
  end

  # Iterate through all records.
  def each_record
    unless block_given?
      return enum_for(:each_record)
    end

    c = record_cursor(:min)

    while rec = c.record
      yield rec
    end

    nil
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:INDEX] = Innodb::Page::Index
