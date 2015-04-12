# -*- encoding : utf-8 -*-

require "innodb/page/index"

# A specialized class for handling INDEX pages, which contain a portion of
# the data from exactly one B+tree. These are typically the most common type
# of page in any database.
#
# The basic structure of an INDEX page is: FIL header, INDEX header, FSEG
# header, fixed-width system records (infimum and supremum), user records
# (the actual data) which grow ascending by offset, free space, the page
# directory which grows descending by offset, and the FIL trailer.
class Innodb::Page::Index::Uncompressed < Innodb::Page::Index
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
    page_header && page_header[:n_dir_slots]
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
    page_header[:garbage_size] +
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

  # A helper function to return the offset to the first free record.
  def garbage_offset
    page_header && page_header[:garbage_offset]
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
        header[:heap_number] = (bits1 & 0xfff8) >> 3
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

      header[:length] = origin - cursor.position
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
        header[:nulls] = cursor.name("nulls") {
          record_header_compact_null_bitmap(cursor)
        }
        header[:lengths], header[:externs] =
          cursor.name("lengths_and_externs") {
            record_header_compact_variable_lengths_and_externs(cursor,
              header[:nulls])
          }
      end
    end
  end

  # Return an array indicating which fields are null.
  def record_header_compact_null_bitmap(cursor)
    fields = record_fields

    # The number of bits in the bitmap is the number of nullable fields.
    size = fields.count { |f| f.nullable? }

    # There is no bitmap if there are no nullable fields.
    return [] unless size > 0

    null_bit_array = cursor.get_bit_array(size).reverse!

    # For every nullable field, select the ones which are actually null.
    fields.inject([]) do |nulls, f|
      nulls << f.name if f.nullable? && (null_bit_array.shift == 1)
      nulls
    end
  end

  # Return an array containing an array of the length of each variable-length
  # field and an array indicating which fields are stored externally.
  def record_header_compact_variable_lengths_and_externs(cursor, nulls)
    fields = (record_format[:key] + record_format[:row])

    lengths = {}
    externs = []

    # For each non-NULL variable-length field, the record header contains
    # the length in one or two bytes.
    fields.each do |f|
      next if !f.variable? || nulls.include?(f.name)

      len = cursor.get_uint8
      ext = false

      # Two bytes are used only if the length exceeds 127 bytes and the
      # maximum length exceeds 255 bytes (or the field is a BLOB type).
      if len > 127 && (f.blob? || f.data_type.width > 255)
        ext = (0x40 & len) != 0
        len = ((len & 0x3f) << 8) + cursor.get_uint8
      end

      lengths[f.name] = len
      externs << f.name if ext
    end

    return lengths, externs
  end

  # Read additional header information from a redundant format record header.
  def record_header_redundant_additional(header, cursor)
    lengths, nulls, externs = [], [], []

    field_offsets = record_header_redundant_field_end_offsets(header, cursor)

    this_field_offset = 0
    field_offsets.each do |n|
      case header[:offset_size]
      when 1
        next_field_offset = (n & RECORD_REDUNDANT_OFF1_OFFSET_MASK)
        lengths << (next_field_offset - this_field_offset)
        nulls   << ((n & RECORD_REDUNDANT_OFF1_NULL_MASK) != 0)
        externs << false
      when 2
        next_field_offset = (n & RECORD_REDUNDANT_OFF2_OFFSET_MASK)
        lengths << (next_field_offset - this_field_offset)
        nulls   << ((n & RECORD_REDUNDANT_OFF2_NULL_MASK) != 0)
        externs << ((n & RECORD_REDUNDANT_OFF2_EXTERN_MASK) != 0)
      end
      this_field_offset = next_field_offset
    end

    # If possible, refer to fields by name rather than position for
    # better formatting (i.e. pp).
    if record_format
      header[:lengths], header[:nulls], header[:externs] = {}, [], []

      record_fields.each do |f|
        header[:lengths][f.name] = lengths[f.position]
        header[:nulls] << f.name if nulls[f.position]
        header[:externs] << f.name if externs[f.position]
      end
    else
      header[:lengths], header[:nulls], header[:externs] = lengths, nulls, externs
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
      Innodb::Record.new(self, {
        :offset => offset,
        :header => header,
        :next => header[:next],
        :data => c.name("data") { c.get_bytes(size_mum_record) },
        :length => c.position - offset,
      })
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

        # Used to indicate whether a field is part of key/row/sys.
        fmap = [:key, :row, :sys].inject({}) do |h, k|
          this_record[k] = []
          record_format[k].each { |f| h[f.position] = k }
          h
        end

        # Read the fields present in this record.
        record_fields.each do |f|
          p = fmap[f.position]
          c.name("#{p.to_s}[#{f.name}]") do
            this_record[p] << {
              :name => f.name,
              :type => f.data_type.name,
              :value => f.value(c, this_record),
              :extern => f.extern(c, this_record),
            }.reject { |k, v| v.nil? }
          end
        end

        # If this is a node (non-leaf) page, it will have a child page number
        # (or "node pointer") stored as the last field.
        if level > 0
          # Read the node pointer in a node (non-leaf) page.
          this_record[:child_page_number] =
            c.name("child_page_number") { c.get_uint32 }
        end

        this_record[:length] = c.position - offset

        # Add system field accessors for convenience.
        this_record[:sys].each do |f|
          case f[:name]
          when "DB_TRX_ID"
            this_record[:transaction_id] = f[:value]
          when "DB_ROLL_PTR"
            this_record[:roll_pointer] = f[:value]
          end
        end
      end

      Innodb::Record.new(self, this_record)
    end
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

  # Return the slot number of the provided offset in the page directory, or nil
  # if the offset is not present in the page directory.
  def offset_is_directory_slot?(offset)
    directory.index(offset)
  end

  # Return the slot number of the provided record in the page directory, or nil
  # if the record is not present in the page directory.
  def record_is_directory_slot?(this_record)
    offset_is_directory_slot?(this_record.offset)
  end

  # Return the slot number for the page directory entry which "owns" the
  # provided record. This will be either the record itself, or the nearest
  # record with an entry in the directory and a value greater than the record.
  def directory_slot_for_record(this_record)
    if slot = record_is_directory_slot?(this_record)
      return slot
    end

    unless search_cursor = record_cursor(this_record.next)
      raise "Couldn't position cursor"
    end

    while rec = search_cursor.record
      if slot = record_is_directory_slot?(rec)
        return slot
      end
    end

    return record_is_directory_slot?(supremum)
  end

  # Return a RecordCursor starting at offset.
  def record_cursor(offset=:min, direction=:forward)
    RecordCursor.new(self, offset, direction)
  end

  def record_if_exists(offset)
    each_record do |rec|
      return rec if rec.offset == offset
    end
  end

  # Return the minimum record on this page.
  def min_record
    min = record(infimum.next)
    min if min != supremum
  end

  # Return the maximum record on this page.
  def max_record
    # Since the records are only singly-linked in the forward direction, in
    # order to do find the last record, we must create a cursor and walk
    # backwards one step.
    unless max_cursor = record_cursor(supremum.offset, :backward)
      raise "Couldn't position cursor"
    end
    # Note the deliberate use of prev_record rather than record; we want
    # to skip over supremum itself.
    max = max_cursor.prev_record
    max if max != infimum
  end

  # Search for a record within a single page, and return either a perfect
  # match for the key, or the last record closest to they key but not greater
  # than the key. (If an exact match is desired, compare_key must be used to
  # check if the returned record matches. This makes the function useful for
  # search in both leaf and non-leaf pages.)
  def linear_search_from_cursor(search_cursor, key)
    Innodb::Stats.increment :linear_search_from_cursor

    this_rec = search_cursor.record

    if Innodb.debug?
      puts "linear_search_from_cursor: page=%i, level=%i, start=(%s)" % [
        offset,
        level,
        this_rec && this_rec.key_string,
      ]
    end

    # Iterate through all records until finding either a matching record or
    # one whose key is greater than the desired key.
    while this_rec && next_rec = search_cursor.record
      Innodb::Stats.increment :linear_search_from_cursor_record_scans

      if Innodb.debug?
        puts "linear_search_from_cursor: page=%i, level=%i, current=(%s)" % [
          offset,
          level,
          this_rec && this_rec.key_string,
        ]
      end

      # If we reach supremum, return the last non-system record we got.
      return this_rec if next_rec.header[:type] == :supremum

      if this_rec.compare_key(key) < 0
        return this_rec
      end

      if (this_rec.compare_key(key) >= 0) &&
        (next_rec.compare_key(key) < 0)
        # The desired key is either an exact match for this_rec or is greater
        # than it but less than next_rec. If this is a non-leaf page, that
        # will mean that the record will fall on the leaf page this node
        # pointer record points to, if it exists at all.
        return this_rec
      end

      this_rec = next_rec
    end

    this_rec
  end

  # Search or a record within a single page using the page directory to limit
  # the number of record comparisons required. Once the last page directory
  # entry closest to but not greater than the key is found, fall back to
  # linear search using linear_search_from_cursor to find the closest record
  # whose key is not greater than the desired key. (If an exact match is
  # desired, the returned record must be checked in the same way as the above
  # linear_search_from_cursor function.)
  def binary_search_by_directory(dir, key)
    Innodb::Stats.increment :binary_search_by_directory

    return nil if dir.empty?

    # Split the directory at the mid-point (using integer math, so the division
    # is rounding down). Retrieve the record that sits at the mid-point.
    mid = ((dir.size-1) / 2)
    rec = record(dir[mid])

    if Innodb.debug?
      puts "binary_search_by_directory: page=%i, level=%i, dir.size=%i, dir[%i]=(%s)" % [
        offset,
        level,
        dir.size,
        mid,
        rec.key_string,
      ]
    end

    # The mid-point record was the infimum record, which is not comparable with
    # compare_key, so we need to just linear scan from here. If the mid-point
    # is the beginning of the page there can't be many records left to check
    # anyway.
    if rec.header[:type] == :infimum
      return linear_search_from_cursor(record_cursor(rec.next), key)
    end

    # Compare the desired key to the mid-point record's key.
    case rec.compare_key(key)
    when 0
      # An exact match for the key was found. Return the record.
      Innodb::Stats.increment :binary_search_by_directory_exact_match
      rec
    when +1
      # The mid-point record's key is less than the desired key.
      if dir.size > 2
        # There are more entries remaining from the directory, recurse again
        # using binary search on the right half of the directory, which
        # represents values greater than or equal to the mid-point record's
        # key.
        Innodb::Stats.increment :binary_search_by_directory_recurse_right
        binary_search_by_directory(dir[mid...dir.size], key)
      else
        next_rec = record(dir[mid+1])
        next_key = next_rec && next_rec.compare_key(key)
        if dir.size == 1 || next_key == -1 || next_key == 0
          # This is the last entry remaining from the directory, or our key is
          # greater than rec and less than rec+1's key. Use linear search to
          # find the record starting at rec.
          Innodb::Stats.increment :binary_search_by_directory_linear_search
          linear_search_from_cursor(record_cursor(rec.offset), key)
        elsif next_key == +1
          Innodb::Stats.increment :binary_search_by_directory_linear_search
          linear_search_from_cursor(record_cursor(next_rec.offset), key)
        else
          nil
        end
      end
    when -1
      # The mid-point record's key is greater than the desired key.
      if dir.size == 1
        # If this is the last entry remaining from the directory, we didn't
        # find anything workable.
        Innodb::Stats.increment :binary_search_by_directory_empty_result
        nil
      else
        # Recurse on the left half of the directory, which represents values
        # less than the mid-point record's key.
        Innodb::Stats.increment :binary_search_by_directory_recurse_left
        binary_search_by_directory(dir[0...mid], key)
      end
    end
  end

  # Iterate through all records in the garbage list.
  def each_garbage_record
    unless block_given?
      return enum_for(:each_garbage_record)
    end

    if garbage_offset == 0
      return nil
    end

    c = record_cursor(garbage_offset)

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
      yield rec.child_page_number, rec.key
    end

    nil
  end

  def each_region
    unless block_given?
      return enum_for(:each_region)
    end

    super do |region|
      yield region
    end

    yield({
      :offset => pos_index_header,
      :length => size_index_header,
      :name   => :index_header,
      :info   => "Index Header",
    })

    yield({
      :offset => pos_fseg_header,
      :length => size_fseg_header,
      :name   => :fseg_header,
      :info   => "File Segment Header",
    })

    yield({
      :offset => pos_infimum - 5,
      :length => size_mum_record + 5,
      :name   => :infimum,
      :info   => "Infimum",
    })

    yield({
      :offset => pos_supremum - 5,
      :length => size_mum_record + 5,
      :name   => :supremum,
      :info   => "Supremum",
    })

    directory_slots.times do |n|
      yield({
        :offset => pos_directory - (n * 2),
        :length => 2,
        :name   => :directory,
        :info   => "Page Directory",
      })
    end

    each_garbage_record do |record|
      yield({
        :offset => record.offset - record.header[:length],
        :length => record.length + record.header[:length],
        :name   => :garbage,
        :info   => "Garbage",
      })
    end

    each_record do |record|
      yield({
        :offset => record.offset - record.header[:length],
        :length => record.header[:length],
        :name   => :record_header,
        :info   => "Record Header",
      })

      yield({
        :offset => record.offset,
        :length => record.length || 1,
        :name   => :record_data,
        :info   => "Record Data",
      })
    end

    nil
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

    puts "page directory:"
    pp directory
    puts

    puts "system records:"
    pp infimum.record
    pp supremum.record
    puts

    puts "garbage records:"
    each_garbage_record do |rec|
      pp rec.record
      puts
    end
    puts

    puts "records:"
    each_record do |rec|
      pp rec.record
      puts
    end
    puts
  end
end
