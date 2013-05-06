require "innodb/fseg_entry"

# A specialized class for handling INDEX pages, which contain a portion of
# the data from exactly one B+tree. These are typically the most common type
# of page in any database.
#
# The basic structure of an INDEX page is: FIL header, INDEX header, FSEG
# header, fixed-width system records (infimum and supremum), user records
# (the actual data) which grow ascending by offset, free space, the page
# directory which grows descending by offset, and the FIL trailer.
class Innodb::Page::Index < Innodb::Page
  attr_accessor :record_describer

  # The size (in bytes) of the "next" pointer in each record header.
  RECORD_NEXT_SIZE = 2

  # The size (in bytes) of the bit-packed fields in each record header for
  # "redundant" record format.
  RECORD_REDUNDANT_BITS_SIZE = 4

  # Masks for 1-byte record end-offsets within "redundant" records.
  RECORD_REDUNDANT_OFF1_OFFSET_MASK   = 0x7f
  RECORD_REDUNDANT_OFF1_NULL_MASK     = 0x80

  # Masks for 2-byte record end-offsets within "redundant" records.
  RECORD_REDUNDANT_OFF2_OFFSET_MASK   = 0x3fff
  RECORD_REDUNDANT_OFF2_NULL_MASK     = 0x8000
  RECORD_REDUNDANT_OFF2_EXTERN_MASK   = 0x4000

  # The size (in bytes) of the bit-packed fields in each record header for
  # "compact" record format.
  RECORD_COMPACT_BITS_SIZE = 3

  # Page direction values possible in the page_header's :direction field.
  PAGE_DIRECTION = {
    1 => :left,           # Inserts have been in descending order.
    2 => :right,          # Inserts have been in ascending order.
    3 => :same_rec,       # Unused by InnoDB.
    4 => :same_page,      # Unused by InnoDB.
    5 => :no_direction,   # Inserts have been in random order.
  }

  # Record types used in the :type field of the record header.
  RECORD_TYPES = {
    0 => :conventional,   # A normal user record in a leaf page.
    1 => :node_pointer,   # A node pointer in a non-leaf page.
    2 => :infimum,        # The system "infimum" record.
    3 => :supremum,       # The system "supremum" record.
  }

  # This record is the minimum record at this level of the B-tree.
  RECORD_INFO_MIN_REC_FLAG = 1

  # This record has been marked as deleted.
  RECORD_INFO_DELETED_FLAG = 2

  # The size (in bytes) of the record pointers in each page directory slot.
  PAGE_DIR_SLOT_SIZE = 2

  # The minimum number of records "owned" by each record with an entry in
  # the page directory.
  PAGE_DIR_SLOT_MIN_N_OWNED = 4

  # The maximum number of records "owned" by each record with an entry in
  # the page directory.
  PAGE_DIR_SLOT_MAX_N_OWNED = 8

  # Return the byte offset of the start of the "index" page header, which
  # immediately follows the "fil" header.
  def pos_index_header
    pos_fil_header + size_fil_header
  end

  # The size of the "index" header.
  def size_index_header
    2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 8 + 2 + 8
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

  # Return the size of the header for each record.
  def size_record_header
    case page_header[:format]
    when :compact
      RECORD_NEXT_SIZE + RECORD_COMPACT_BITS_SIZE
    when :redundant
      RECORD_NEXT_SIZE + RECORD_REDUNDANT_BITS_SIZE
    end
  end

  # The size of the additional data structures in the header of the system
  # records, which is just 1 byte in redundant format to store the offset
  # of the end of the field. This is needed specifically here since we need
  # to be able to calculate the fixed positions of these system records.
  def size_mum_record_header_additional
    case page_header[:format]
    when :compact
      0 # No additional data is stored in compact format.
    when :redundant
      1 # A 1-byte offset for 1 field is stored in redundant format.
    end
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
    pos_records +
      size_record_header +
      size_mum_record_header_additional
  end

  # Return the byte offset of the start of the "origin" of the supremum record,
  # which is always the last record in the singly-linked record chain on any
  # page, and represents a record with a "higher value than any possible user
  # record". The supremum record immediately follows the infimum record.
  def pos_supremum
    pos_infimum +
      size_record_header +
      size_mum_record_header_additional +
      size_mum_record
  end

  # Return the byte offset of the start of records within the page (the
  # position immediately after the page header).
  def pos_records
    size_fil_header +
      size_index_header +
      size_fseg_header
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

  # Return the "index" header.
  def page_header
    @page_header ||= cursor(pos_index_header).name("index") do |c|
      index = {
        :n_dir_slots    => c.name("n_dir_slots") { c.get_uint16 },
        :heap_top       => c.name("heap_top") { c.get_uint16 },
        :n_heap_format  => c.name("n_heap_format") { c.get_uint16 },
        :free           => c.name("free") { c.get_uint16 },
        :garbage        => c.name("garbage") { c.get_uint16 },
        :last_insert    => c.name("last_insert") { c.get_uint16 },
        :direction      => c.name("direction") { PAGE_DIRECTION[c.get_uint16] },
        :n_direction    => c.name("n_direction") { c.get_uint16 },
        :n_recs         => c.name("n_recs") { c.get_uint16 },
        :max_trx_id     => c.name("max_trx_id") { c.get_uint64 },
        :level          => c.name("level") { c.get_uint16 },
        :index_id       => c.name("index_id") { c.get_uint64 },
      }
      index[:n_heap] = index[:n_heap_format] & (2**15-1)
      index[:format] = (index[:n_heap_format] & 1<<15) == 0 ?
        :redundant : :compact
      index.delete :n_heap_format

      index
    end
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

  # Return the header from a record.
  def record_header(cursor)
    origin = cursor.position
    header = {}
    cursor.backward.name("header") do |c|
      case page_header[:format]
      when :compact
        # The "next" pointer is a relative offset from the current record.
        header[:next] = c.name("next") { origin + c.get_sint16 }

        # Fields packed in a 16-bit integer (LSB first):
        #   3 bits for type
        #   13 bits for heap_number
        bits1 = c.name("bits1") { c.get_uint16 }
        header[:type] = RECORD_TYPES[bits1 & 0x07]
        header[:heap_number] = (bits1 & 0xf8) >> 3
      when :redundant
        # The "next" pointer is an absolute offset within the page.
        header[:next] = c.name("next") { c.get_uint16 }

        # Fields packed in a 24-bit integer (LSB first):
        #   1 bit for offset_size (0 = 2 bytes, 1 = 1 byte)
        #   10 bits for n_fields
        #   13 bits for heap_number
        bits1 = c.name("bits1") { c.get_uint24 }
        header[:offset_size]  = (bits1 & 1) == 0 ? 2 : 1
        header[:n_fields]     = (bits1 & (((1 << 10) - 1) <<  1)) >>  1
        header[:heap_number]  = (bits1 & (((1 << 13) - 1) << 11)) >> 11
      end

      # Fields packed in an 8-bit integer (LSB first):
      #   4 bits for n_owned
      #   4 bits for flags
      bits2 = c.name("bits2") { c.get_uint8 }
      header[:n_owned] = bits2 & 0x0f
      info = (bits2 & 0xf0) >> 4
      header[:min_rec] = (info & RECORD_INFO_MIN_REC_FLAG) != 0
      header[:deleted] = (info & RECORD_INFO_DELETED_FLAG) != 0

      case page_header[:format]
      when :compact
        record_header_compact_additional(header, cursor)
      when :redundant
        record_header_redundant_additional(header, cursor)
      end
    end

    header
  end

  # Read additional header information from a compact format record header.
  def record_header_compact_additional(header, cursor)
    case header[:type]
    when :conventional, :node_pointer
      # The variable-length part of the record header contains a
      # bit vector indicating NULL fields and the length of each
      # non-NULL variable-length field.
      if record_format
        header[:field_nulls] = cursor.name("field_nulls") {
          record_header_compact_null_bitmap(cursor)
        }
        header[:field_lengths] = cursor.name("field_lengths") {
          record_header_compact_variable_lengths(cursor, header[:field_nulls])
        }
      end
    end
  end

  # Return an array indicating which fields are null.
  def record_header_compact_null_bitmap(cursor)
    fields = (record_format[:key] + record_format[:row])

    # The number of bits in the bitmap is the number of nullable fields.
    size = fields.count do |f| f.nullable end

    # There is no bitmap if there are no nullable fields.
    return nil unless size > 0

    # To simplify later checks, expand bitmap to one for each field.
    bitmap = Array.new(fields.size, false)

    null_bit_array = cursor.get_bit_array(size).reverse!

    # For every nullable field, set whether the field is actually null.
    fields.each do |f|
      bitmap[f.position] = f.nullable ? (null_bit_array.shift == 1) : false
    end

    return bitmap
  end

  # Return an array containing the length of each variable-length field.
  def record_header_compact_variable_lengths(cursor, null_bitmap)
    fields = (record_format[:key] + record_format[:row])

    len_array = Array.new(fields.size, 0)

    # For each non-NULL variable-length field, the record header contains
    # the length in one or two bytes.
    fields.each do |f|
      next if f.fixed_len > 0 or null_bitmap[f.position]

      len = cursor.get_uint8

      # Two bytes are used only if the length exceeds 127 bytes and the
      # maximum length exceeds 255 bytes.
      if len > 127 and f.variable_len > 255
        len = ((len & 0x3f) << 8) + cursor.get_uint8
      end

      len_array[f.position] = len
    end

    return len_array
  end

  # Read additional header information from a redundant format record header.
  def record_header_redundant_additional(header, cursor)
    header[:field_lengths] = []
    header[:field_nulls] = []
    header[:field_externs] = []

    field_offsets = record_header_redundant_field_end_offsets(header, cursor)

    this_field_offset = 0
    field_offsets.each do |n|
      case header[:offset_size]
      when 1
        next_field_offset = (n & RECORD_REDUNDANT_OFF1_OFFSET_MASK)
        header[:field_lengths]  << (next_field_offset - this_field_offset)
        header[:field_nulls]    << ((n & RECORD_REDUNDANT_OFF1_NULL_MASK) != 0)
        header[:field_externs]  << false
      when 2
        next_field_offset = (n & RECORD_REDUNDANT_OFF2_OFFSET_MASK)
        header[:field_lengths]  << (next_field_offset - this_field_offset)
        header[:field_nulls]    << ((n & RECORD_REDUNDANT_OFF2_NULL_MASK) != 0)
        header[:field_externs]  << ((n & RECORD_REDUNDANT_OFF2_EXTERN_MASK) != 0)
      end
      this_field_offset = next_field_offset
    end
  end

  # Read field end offsets from the provided cursor for each field as counted
  # by n_fields.
  def record_header_redundant_field_end_offsets(header, cursor)
    (0...header[:n_fields]).to_a.inject([]) do |offsets, n|
      cursor.name("field_end_offset[#{n}]") {
        offsets << cursor.get_uint_by_size(header[:offset_size])
      }
      offsets
    end
  end

  # Parse and return simple fixed-format system records, such as InnoDB's
  # internal infimum and supremum records.
  def system_record(offset)
    cursor(offset).name("record[#{offset}]") do |c|
      header = c.peek { record_header(c) }
      {
        :offset => offset,
        :header => header,
        :next => header[:next],
        :data => c.name("data") { c.get_bytes(size_mum_record) },
      }
    end
  end

  # Return the infimum record on a page.
  def infimum
    @infimum ||= system_record(pos_infimum)
  end

  # Return the supremum record on a page.
  def supremum
    @supremum ||= system_record(pos_supremum)
  end

  # Return a set of field objects that describe the record.
  def make_record_description
    description = record_describer.cursor_sendable_description(self)

    position = 0
    fields = {:type => description[:type], :key => [], :row => []}

    description[:key].each_with_index do |d|
      fields[:key] << Innodb::Field.new(position, *d)
      position += 1
    end

    # Account for TRX_ID and ROLL_PTR.
    position += 2

    description[:row].each_with_index do |d|
      fields[:row] << Innodb::Field.new(position, *d)
      position += 1
    end

    fields
  end

  # Return (and cache) the record format provided by an external class.
  def record_format
    if record_describer
      @record_format ||= make_record_description()
    end
  end

  # Parse and return a record at a given offset.
  def record(offset)
    return nil unless offset
    return infimum  if offset == pos_infimum
    return supremum if offset == pos_supremum

    cursor(offset).forward.name("record[#{offset}]") do |c|
      # There is a header preceding the row itself, so back up and read it.
      header = c.peek { record_header(c) }

      this_record = {
        :format => page_header[:format],
        :offset => offset,
        :header => header,
        :next => header[:next] == 0 ? nil : (header[:next]),
      }

      if record_format
        this_record[:type] = record_format[:type]

        # Read the key fields present in all types of pages.
        this_record[:key] = []
        c.name("key") do
          record_format[:key].each do |f|
            this_record[:key].push f.read(this_record, c)
          end
        end

        # If this is a leaf page of the clustered index, read InnoDB's internal
        # fields, a transaction ID and roll pointer.
        if level == 0 && record_format[:type] == :clustered
          this_record[:transaction_id] = c.name("transaction_id") { c.get_hex(6) }
          c.name("roll_pointer") do
            rseg_id_insert_flag = c.name("rseg_id_insert_flag") { c.get_uint8 }
            this_record[:roll_pointer]   = {
              :is_insert  => (rseg_id_insert_flag & 0x80) == 0x80,
              :rseg_id    => rseg_id_insert_flag & 0x7f,
              :undo_log   => c.name("undo_log") {
                {
                  :page   => c.name("page")   { c.get_uint32 },
                  :offset => c.name("offset") { c.get_uint16 },
                }
              }
            }
          end
        end

        # If this is a leaf page of the clustered index, or any page of a
        # secondary index, read the non-key fields.
        if (level == 0 && record_format[:type] == :clustered) ||
          (record_format[:type] == :secondary)
          # Read the non-key fields.
          this_record[:row] = []
          c.name("row") do
            record_format[:row].each do |f|
              this_record[:row].push f.read(this_record, c)
            end
          end
        end

        # If this is a node (non-leaf) page, it will have a child page number
        # (or "node pointer") stored as the last field.
        if level > 0
          # Read the node pointer in a node (non-leaf) page.
          this_record[:child_page_number] =
            c.name("child_page_number") { c.get_uint32 }
        end
      end

      this_record
    end
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
    unless block_given?
      return enum_for(:each_record)
    end

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

    unless block_given?
      return enum_for(:each_child_page)
    end

    each_record do |rec|
      yield rec[:child_page_number], rec[:key]
    end

    nil
  end

  # Return an array of row offsets for all entries in the page directory.
  def directory
    return @directory if @directory

    @directory = []
    cursor(pos_directory).backward.name("page_directory") do |c|
      directory_slots.times do |n|
        @directory.push c.name("slot[#{n}]") { c.get_uint16 }
      end
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
