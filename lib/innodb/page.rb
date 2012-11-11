require "innodb/page_cursor"

class Innodb::Page
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

  PAGE_DIRECTION = {
    1 => :left,
    2 => :right,
    3 => :same_rec,
    4 => :same_page,
    5 => :no_direction,
  }

  def initialize(page)
    @page = page
  end

  def cursor(offset)
    Cursor.new(self, offset)
  end

  FIL_HEADER_START  = 0
  FIL_HEADER_SIZE   = 38

  PAGE_HEADER_START = FIL_HEADER_START + FIL_HEADER_SIZE
  PAGE_HEADER_SIZE  = 36

  FSEG_HEADER_START = PAGE_HEADER_START + PAGE_HEADER_SIZE
  FSEG_HEADER_SIZE  = 10
  FSEG_HEADER_COUNT = 2

  MUM_RECORD_SIZE   = 8
  
  RECORD_BITS_SIZE  = 3
  RECORD_NEXT_SIZE  = 2

  def size_record_header
    case page_header[:format]
    when :compact
      RECORD_BITS_SIZE + RECORD_NEXT_SIZE
    when :redundant
      RECORD_BITS_SIZE + RECORD_NEXT_SIZE + 1
    end
  end

  def size_record_undefined
    case page_header[:format]
    when :compact
      0
    when :redundant
      1
    end
  end

  def pos_records
    FIL_HEADER_SIZE + 
      PAGE_HEADER_SIZE + 
      (FSEG_HEADER_COUNT * FSEG_HEADER_SIZE)
  end

  def pos_infimum
    pos_records + size_record_header + size_record_undefined
  end

  def pos_supremum
    pos_infimum + size_record_header + size_record_undefined + MUM_RECORD_SIZE
  end

  def pos_user_records
    pos_supremum + size_record_header + size_record_undefined + MUM_RECORD_SIZE
  end

  def maybe_undefined(value)
    value == 4294967295 ? nil : value
  end

  def data(offset, length)
    @page[offset...(offset + length)]
  end

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

  def type
    fil_header[:type]
  end

  def prev
    fil_header[:prev]
  end

  def next
    fil_header[:next]
  end

  def page_header
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

  def system_record(offset)
    return nil unless fil_header[:type] == :INDEX

    c = cursor(offset)
    c.adjust(-2)
    {
      :next => offset + c.get_sint16,
      :data => c.get_bytes(8),
    }
  end

  def infimum
    @infimum ||= system_record(pos_infimum)
  end

  def supremum
    @supremum ||= system_record(pos_supremum)
  end

  def record_header(offset)
    c = cursor(offset).backward
    case page_header[:format]
    when :compact
      {
        :next => c.get_sint16,
        :bits1 => c.get_uint16,
        :bits2 => c.get_uint8,
      }
    when :redundant
      raise "Not implemented"
    end

  end

  def record(offset)
    return nil unless offset
    return nil if offset == pos_infimum
    return nil if offset == pos_supremum

    c = cursor(offset)
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

  def each_record
    rec = infimum
    while rec = record(rec[:next])
      yield rec
    end
    nil
  end

  def dump
    puts
    puts "fil header:"
    pp fil_header

    puts
    puts "page header:"
    pp page_header

    if fil_header[:type] == :INDEX
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
