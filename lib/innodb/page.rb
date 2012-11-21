require "innodb/cursor"

class Innodb::Page
  # Currently only 16kB InnoDB pages are supported.
  PAGE_SIZE = 16384

  # InnoDB Page Type constants from include/fil0fil.h.
  PAGE_TYPE = {
    0     => :ALLOCATED,      # Freshly allocated page
    2     => :UNDO_LOG,       # Undo log page
    3     => :INODE,          # Index node
    4     => :IBUF_FREE_LIST, # Insert buffer free list
    5     => :IBUF_BITMAP,    # Insert buffer bitmap
    6     => :SYS,            # System page
    7     => :TRX_SYS,        # Transaction system data
    8     => :FSP_HDR,        # File space header
    9     => :XDES,           # Extent descriptor page
    10    => :BLOB,           # Uncompressed BLOB page
    11    => :ZBLOB,          # First compressed BLOB page
    12    => :ZBLOB2,         # Subsequent compressed BLOB page
    17855 => :INDEX,          # B-tree node
  }

  # Initialize a page by passing in a 16kB buffer containing the raw page
  # contents. Currently only 16kB pages are supported.
  def initialize(buffer)
    unless buffer.size == PAGE_SIZE
      raise "Page buffer provided was not #{PAGE_SIZE} bytes" 
    end

    @buffer = buffer
  end

  # A helper function to return bytes from the page buffer based on offset
  # and length, both in bytes.
  def data(offset, length)
    @buffer[offset...(offset + length)]
  end

  # Return an Innodb::Cursor object positioned at a specific offset.
  def cursor(offset)
    Innodb::Cursor.new(self, offset)
  end

  FIL_HEADER_SIZE   = 38
  FIL_HEADER_START  = 0

  # A helper to convert "undefined" values stored in previous and next pointers
  # in the page header to nil.
  def maybe_undefined(value)
    value == 4294967295 ? nil : value
  end

  # Return the "fil" header from the page, which is common for all page types.
  def fil_header
    c = cursor(FIL_HEADER_START)
    @fil_header ||= {
      :checksum   => c.get_uint32,
      :offset     => c.get_uint32,
      :prev       => maybe_undefined(c.get_uint32),
      :next       => maybe_undefined(c.get_uint32),
      :lsn        => c.get_uint64,
      :type       => PAGE_TYPE[c.get_uint16],
      :flush_lsn  => c.get_uint64,
      :space_id   => c.get_uint32,
    }
  end
  alias :fh :fil_header

  # A helper function to return the page type from the "fil" header, for easier
  # access.
  def type
    fil_header[:type]
  end

  # A helper function to return the page number of the logical previous page
  # (from the doubly-linked list from page to page) from the "fil" header,
  # for easier access.
  def prev
    fil_header[:prev]
  end

  # A helper function to return the page number of the logical next page
  # (from the doubly-linked list from page to page) from the "fil" header,
  # for easier access.
  def next
    fil_header[:next]
  end

  PAGE_HEADER_SIZE  = 36
  PAGE_HEADER_START = FIL_HEADER_START + FIL_HEADER_SIZE

  PAGE_TRAILER_SIZE  = 16
  PAGE_TRAILER_START = PAGE_SIZE - PAGE_TRAILER_SIZE

  FSEG_HEADER_SIZE  = 10
  FSEG_HEADER_START = PAGE_HEADER_START + PAGE_HEADER_SIZE
  FSEG_HEADER_COUNT = 2

  MUM_RECORD_SIZE   = 8
  
  RECORD_BITS_SIZE  = 3
  RECORD_NEXT_SIZE  = 2

  # Page direction values possible in the page_header[:direction] field.
  PAGE_DIRECTION = {
    1 => :left,
    2 => :right,
    3 => :same_rec,
    4 => :same_page,
    5 => :no_direction,
  }

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

  # Return the "page" header; currently only "INDEX" pages are supported.
  def page_header
    return nil unless type == :INDEX

    c = cursor(PAGE_HEADER_START)
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
  alias :ph :page_header

  # Parse and return simple fixed-format system records, such as InnoDB's
  # internal infimum and supremum records.
  def system_record(offset)
    return nil unless type == :INDEX

    c = cursor(offset)
    c.adjust(-2)
    {
      :next => offset + c.get_sint16,
      :data => c.get_bytes(8),
    }
  end

  # Return the byte offset of the start of the "origin" of the infimum record,
  # which is always the first record in the singly-linked record chain on any
  # page, and represents a record with a "lower value than any possible user
  # record". The infimum record immediately follows the page header.
  def pos_infimum
    pos_records + size_record_header + size_record_undefined
  end

  # Return the infimum record on a page.
  def infimum
    @infimum ||= system_record(pos_infimum)
  end

  # Return the byte offset of the start of the "origin" of the supremum record,
  # which is always the last record in the singly-linked record chain on any
  # page, and represents a record with a "higher value than any possible user
  # record". The supremum record immediately follows the infimum record.
  def pos_supremum
    pos_infimum + size_record_header + size_record_undefined + MUM_RECORD_SIZE
  end

  # Return the supremum record on a page.
  def supremum
    @supremum ||= system_record(pos_supremum)
  end

  # Return the byte offset of the start of records within the page (the
  # position immediately after the page header).
  def pos_records
    FIL_HEADER_SIZE + 
      PAGE_HEADER_SIZE + 
      (FSEG_HEADER_COUNT * FSEG_HEADER_SIZE)
  end

  # Return the byte offset of the start of the user records in a page, which
  # immediately follows the supremum record.
  def pos_user_records
    pos_supremum + size_record_header + size_record_undefined + MUM_RECORD_SIZE
  end

  # Return the amount of free space in the page.
  def free_space
    unused_space = (PAGE_TRAILER_START - page_header[:heap_top])
    unused_space + page_header[:garbage]
  end

  # Return the amount of used space in the page.
  def used_space
    PAGE_SIZE - free_space
  end

  # Return the amount of space occupied by records in the page.
  def record_space
    used_space - pos_user_records
  end

  # Return the actual bytes of the portion of the page which is used to
  # store user records (eliminate the headers and trailer from the page).
  def record_bytes
    data(pos_user_records, page_header[:heap_top] - pos_user_records)
  end

  # Return the header from a record. (This is mostly unimplemented.)
  def record_header(offset)
    return nil unless type == :INDEX

    c = cursor(offset).backward
    case page_header[:format]
    when :compact
      header = {}
      header[:next] = c.get_sint16
      bits1 = c.get_uint16
      header[:type] = bits1 & 0x07
      header[:order] = (bits1 & 0xf8) >> 3
      bits2 = c.get_uint8
      header[:n_owned] = bits2 & 0x0f
      header[:deleted] = (bits2 & 0xf0) >> 4
      header
    when :redundant
      raise "Not implemented"
    end
  end

  # Return a record. (This is mostly unimplemented.)
  def record(offset)
    return nil unless offset
    return nil unless type == :INDEX
    return nil if offset == pos_infimum
    return nil if offset == pos_supremum

    c = cursor(offset).forward
    # There is a header preceding the row itself, so back up and read it.
    header = record_header(offset)
    {
      :header => header,
      :next => header[:next] == 0 ? nil : (offset + header[:next]),
      # These system records may not be present depending on schema.
      :rec1 => c.get_bytes(6),
      :rec2 => c.get_bytes(6),
      :rec3 => c.get_bytes(7),
      # Read a few bytes just so it can be visually verified.
      :data => c.get_bytes(8),
    }
  end

  # Iterate through all records. (This is mostly unimplemented.)
  def each_record
    rec = infimum
    while rec = record(rec[:next])
      yield rec
    end
    nil
  end

  # Dump the contents of a page for debugging purposes.
  def dump
    puts
    puts "fil header:"
    pp fil_header

    puts
    puts "page header:"
    pp page_header

    puts
    puts "free space: #{free_space}"
    puts "used space: #{used_space}"
    puts "record space: #{record_space}"

    if type == :INDEX
      puts
      puts "system records:"
      pp infimum
      pp supremum
      
      puts
      puts "records:"
      each_record do |rec|
        pp rec
      end
    end
  end
end
