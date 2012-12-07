require "innodb/fseg_entry"

class Innodb::Page::Index < Innodb::Page
  attr_accessor :record_describer

  # Return the byte offset of the start of the "index" page header, which
  # immediately follows the "fil" header.
  def pos_index_header
    pos_fil_header + size_fil_header
  end

  # The size of the "index" header.
  def size_index_header
    36
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

  # Return the byte offset of the start of records within the page (the
  # position immediately after the page header).
  def pos_records
    size_fil_header + size_index_header + size_fseg_header
  end

  # The size of the data from the supremum or infimum records.
  def size_mum_record
    8
  end

  # Return the byte offset of the start of the "origin" of the infimum record,
  # which is always the first record in the singly-linked record chain on any
  # page, and represents a record with a "lower value than any possible user
  # record". The infimum record immediately follows the page header.
  def pos_infimum
    pos_records + size_record_header + size_record_undefined
  end

  # Return the byte offset of the start of the "origin" of the supremum record,
  # which is always the last record in the singly-linked record chain on any
  # page, and represents a record with a "higher value than any possible user
  # record". The supremum record immediately follows the infimum record.
  def pos_supremum
    pos_infimum + size_record_header + size_record_undefined + size_mum_record
  end

  # Return the byte offset of the start of the user records in a page, which
  # immediately follows the supremum record.
  def pos_user_records
    pos_supremum + size_mum_record
  end

  # The position of the page directory, which starts at the "fil" trailer and
  # grows backwards from there.
  def pos_directory
    pos_fil_trailer
  end

  # The amount of space consumed by the page header.
  def header_space
    # The end of the supremum system record is the beginning of the space
    # available for user records.
    pos_user_records
  end

  # The number of directory slots in use.
  def directory_slots
    page_header[:n_dir_slots]
  end

  # The amount of space consumed by the page directory.
  def directory_space
    directory_slots * PAGE_DIR_SLOT_SIZE
  end

  # The amount of space consumed by the trailers in the page.
  def trailer_space
    size_fil_trailer
  end

  # Return the amount of free space in the page.
  def free_space
    page_header[:garbage] +
      (size - size_fil_trailer - directory_space - page_header[:heap_top])
  end

  # Return the amount of used space in the page.
  def used_space
    size - free_space
  end

  # Return the amount of space occupied by records in the page.
  def record_space
    used_space - header_space - directory_space - trailer_space
  end

  # Return the actual bytes of the portion of the page which is used to
  # store user records (eliminate the headers and trailer from the page).
  def record_bytes
    data(pos_user_records, page_header[:heap_top] - pos_user_records)
  end

  # Page direction values possible in the page_header[:direction] field.
  PAGE_DIRECTION = {
    1 => :left,
    2 => :right,
    3 => :same_rec,
    4 => :same_page,
    5 => :no_direction,
  }

  # Return the "index" header.
  def page_header
    c = cursor(pos_index_header)
    @page_header ||= {
      :n_dir_slots  => c.get_uint16,
      :heap_top     => c.get_uint16,
      :n_heap       => ((n_heap = c.get_uint16) & (2**15-1)),
      :free         => c.get_uint16,
      :garbage      => c.get_uint16,
      :last_insert  => c.get_uint16,
      :direction    => PAGE_DIRECTION[c.get_uint16],
      :n_direction  => c.get_uint16,
      :n_recs       => c.get_uint16,
      :max_trx_id   => c.get_uint64,
      :level        => c.get_uint16,
      :index_id     => c.get_uint64,
      :format       => (n_heap & 1<<15) == 0 ? :redundant : :compact,
    }
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

  # A helper function to identify root index pages; they must be the only pages
  # at their level.
  def root?
    self.prev.nil? && self.next.nil?
  end

  # Return the "fseg" header.
  def fseg_header
    c = cursor(pos_fseg_header)
    @fseg_header ||= {
      :free_list      => Innodb::FsegEntry.get_entry(c),
      :btree_segment  => Innodb::FsegEntry.get_entry(c),
    }
  end

  RECORD_BITS_SIZE  = 3
  RECORD_NEXT_SIZE  = 2

  PAGE_DIR_SLOT_SIZE          = 2
  PAGE_DIR_SLOT_MIN_N_OWNED   = 4
  PAGE_DIR_SLOT_MAX_N_OWNED   = 8

  # Return the size of the header for each record.
  def size_record_header
    case page_header[:format]
    when :compact
      RECORD_BITS_SIZE + RECORD_NEXT_SIZE
    when :redundant
      RECORD_BITS_SIZE + RECORD_NEXT_SIZE + 1
    end
  end

  # Return the size of a field in the record header for which no description
  # could be found (but must be skipped anyway).
  def size_record_undefined
    case page_header[:format]
    when :compact
      0
    when :redundant
      1
    end
  end

  # Record types used in the :type field of the record header.
  RECORD_TYPES = {
    0 => :conventional,
    1 => :node_pointer,
    2 => :infimum,
    3 => :supremum,
  }

  # This record is the minimum record at this level of the B-tree.
  RECORD_INFO_MIN_REC_FLAG = 1

  # This record has been marked as deleted.
  RECORD_INFO_DELETED_FLAG = 2

  # Return the header from a record. (This is mostly unimplemented.)
  def record_header(offset)
    return nil unless type == :INDEX

    c = cursor(offset).backward
    case page_header[:format]
    when :compact
      header = {}
      header[:next] = c.get_sint16
      bits1 = c.get_uint16
      header[:type] = RECORD_TYPES[bits1 & 0x07]
      header[:order] = (bits1 & 0xf8) >> 3
      bits2 = c.get_uint8
      header[:n_owned] = bits2 & 0x0f
      info = (bits2 & 0xf0) >> 4
      header[:min_rec] = (info & RECORD_INFO_MIN_REC_FLAG) != 0
      header[:deleted] = (info & RECORD_INFO_DELETED_FLAG) != 0
      header
    when :redundant
      raise "Not implemented"
    end
  end

  # Parse and return simple fixed-format system records, such as InnoDB's
  # internal infimum and supremum records.
  def system_record(offset)
    return nil unless type == :INDEX

    header = record_header(offset)
    {
      :header => header,
      :next => offset + header[:next],
      :data => cursor(offset).get_bytes(size_mum_record),
    }
  end

  # Return the infimum record on a page.
  def infimum
    @infimum ||= system_record(pos_infimum)
  end

  # Return the supremum record on a page.
  def supremum
    @supremum ||= system_record(pos_supremum)
  end

  # Return (and cache) the record format provided by an external class.
  def record_format
    if record_describer
      @record_format ||= record_describer.cursor_sendable_description(self)
    end
  end

  # Parse and return a record at a given offset.
  def record(offset)
    return nil unless offset
    return nil unless type == :INDEX
    return infimum  if offset == pos_infimum
    return supremum if offset == pos_supremum

    c = cursor(offset).forward

    # There is a header preceding the row itself, so back up and read it.
    header = record_header(offset)

    this_record = {
      :header => header,
      :next => header[:next] == 0 ? nil : (offset + header[:next]),
    }

    if record_format
      this_record[:type] = record_format[:type]

      # Read the key fields present in all types of pages.
      this_record[:key] = []
      record_format[:key].each do |f|
        this_record[:key].push c.send(*f)
      end

      # If this is a leaf page of the clustered index, read InnoDB's internal
      # fields, a transaction ID and roll pointer.
      if level == 0 && record_format[:type] == :clustered
        this_record[:transaction_id] = c.get_hex(6)
        this_record[:roll_pointer]   = c.get_hex(7)
      end

      # If this is a leaf page of the clustered index, or any page of a
      # secondary index, read the non-key fields.
      if (level == 0 && record_format[:type] == :clustered) ||
        (record_format[:type] == :secondary)
        # Read the non-key fields.
        this_record[:row] = []
        record_format[:row].each do |f|
          this_record[:row].push c.send(*f)
        end
      end

      # If this is a node (non-leaf) page, it will have a child page number
      # (or "node pointer") stored as the last field.
      if level > 0
        # Read the node pointer in a node (non-leaf) page.
        this_record[:child_page_number] = c.get_uint32
      end
    end

    this_record
  end

  # A class for cursoring through records starting from an arbitrary point.
  class RecordCursor
    def initialize(page, offset)
      @page   = page
      @offset = offset
    end

    # Return the next record, and advance the cursor. Return nil when the
    # end of records is reached.
    def record
      return nil unless @offset

      record = @page.record(@offset)

      if record == @page.supremum
        @offset = nil
      else
        @offset = record[:next]
        record
      end
    end
  end

  # Return a RecordCursor starting at offset.
  def record_cursor(offset)
    RecordCursor.new(self, offset)
  end

  # Return the first record on this page.
  def first_record
    first = record(infimum[:next])
    first if first != supremum
  end

  # Iterate through all records.
  def each_record
    c = record_cursor(infimum[:next])

    while rec = c.record
      yield rec
    end

    nil
  end

  # Iterate through all child pages of a node (non-leaf) page, which are
  # stored as records with the child page number as the last field in the
  # record.
  def each_child_page
    return nil if level == 0

    each_record do |rec|
      yield rec[:child_page_number], rec[:key]
    end

    nil
  end

  # Return an array of row offsets for all entries in the page directory.
  def directory
    return @directory if @directory

    @directory = []
    c = cursor(pos_directory).backward
    directory_slots.times do
      @directory.push c.get_uint16
    end

    @directory
  end

  # Dump the contents of a page for debugging purposes.
  def dump
    super

    puts "page header:"
    pp page_header
    puts

    puts "fseg header:"
    pp fseg_header
    puts

    puts "sizes:"
    puts "  %-15s%5i" % [ "header",     header_space ]
    puts "  %-15s%5i" % [ "trailer",    trailer_space ]
    puts "  %-15s%5i" % [ "directory",  directory_space ]
    puts "  %-15s%5i" % [ "free",       free_space ]
    puts "  %-15s%5i" % [ "used",       used_space ]
    puts "  %-15s%5i" % [ "record",     record_space ]
    puts "  %-15s%5.2f" % [
      "per record",
      (page_header[:n_recs] > 0) ? (record_space / page_header[:n_recs]) : 0
    ]
    puts

    puts "system records:"
    pp infimum
    pp supremum
    puts

    puts "page directory:"
    pp directory
    puts

    puts "records:"
    each_record do |rec|
      pp rec
    end
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:INDEX] = Innodb::Page::Index