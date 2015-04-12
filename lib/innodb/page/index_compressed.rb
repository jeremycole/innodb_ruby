# -*- encoding : utf-8 -*-

require "innodb/page/index"
require "zlib"

# A specialized class for handling compressed INDEX pages.
class Innodb::Page::Index::Compressed < Innodb::Page::Index
  # The size (in bytes) of an entry in the dense page directory.
  PAGE_ZIP_DIR_SLOT_SIZE  = 2

  # Masks for flags on dense directory slots.
  PAGE_ZIP_DIR_SLOT_MASK  = 0x3fff
  PAGE_ZIP_DIR_SLOT_OWNED = 0x4000
  PAGE_ZIP_DIR_SLOT_DEL   = 0x8000

  # Decompress the compressed parts of the page. The first block of
  # uncompressed data (flushed during compression) contains index
  # information for the compressed page. The block after is made
  # of data from records.
  def inflate_blocks
    return @inflated_blocks if @inflated_blocks
    zd_data = @buffer.byteslice(pos_compressed_data, @buffer.size)
    zi_stream = Zlib::Inflate.new
    zd_data.each_byte do |byte|
      zi_stream << byte.chr
      break if zi_stream.sync_point?
    end
    block1 = [zi_stream.flush_next_out, zi_stream.total_in]
    total_out = zi_stream.total_out
    zi_stream << zd_data.byteslice(zi_stream.total_in, zd_data.size)
    next_out = zi_stream.finish
    block2 = [next_out[0, zi_stream.total_out - total_out], zi_stream.total_in]
    @inflated_blocks = [block1, block2].transpose
  end

  # Return an array contain the inflated blocks of the page.
  def inflate_block
    inflate_blocks[0]
  end

  # Return an array containing the amount of compressed data read.
  def deflate_block_boundary
    inflate_blocks[1]
  end

  # Return the amount of used space in the page.
  def used_space
    header_space +
      compressed_space +
      modification_log_space +
      trailer_space
  end

  # Return the amount of free space in the page.
  def free_space
    size - used_space
  end

  # Return the amount of space occupied by records in the page.
  def record_space
    compressed_space +
      modification_log_space
  end

  # Return the amount of space occupied by compressed data
  def compressed_space
    deflate_block_boundary[1]
  end

  # Return the amount of space occupied by the modification log.
  def modification_log_space
    modification_log[:length]
  end

  # The amount of space consumed by the trailers in the page.
  def trailer_space
    length = 0
    if not leaf?
      length = SYS_FIELD_NODE_PTR_LENGTH
    else clustered?
      length = SYS_FIELD_TRX_ID_LENGTH + SYS_FIELD_ROLL_PTR_LENGTH
    end
    directory_space + (directory_slots * length)
    # + (n_blobs * Innodb::Field::EXTERN_FIELD_SIZE)
  end

  # Return the byte offset of the start of compressed data within
  # the page (the position immediately after the page header).
  def pos_compressed_data
    header_space
  end

  # Return the byte offset of the start of the modification log
  # within the page (the position after the compressed data).
  def pos_modification_log
    pos_compressed_data +
      compressed_space
  end

  # The position of the dense page directory, which starts at the
  # end of the page and grows backwards from there.
  def pos_directory
    size
  end

  # The number of directory slots in use.
  def directory_slots
    records
  end

  # The number of directory slots in the free list.
  def directory_free_slots
    page_header[:n_heap] - 2 - directory_slots
  end

  # The amount of space consumed by the page directory.
  def directory_space
    directory_slots * PAGE_ZIP_DIR_SLOT_SIZE
  end

  # A helper function to identify clustered index pages.
  def clustered?
    index_fields.fetch(:trx_id_position, 0) != 0
  end

  # Read the page index information.
  def index_fields
    return @index_fields if @index_fields

    values = inflate_block[0].bytes
    num_fields = -1
    loop do
      value = values.next rescue break
      values.next if (value & 0x80) != 0
      num_fields += 1
    end

    fields = []
    values.rewind

    num_fields.downto(1) do
      value = values.next
      if (value & 0x80) != 0
        value = (value & 0x7f) << 8 | values.next
        length = value >> 1
        type = :FIXBINARY
      elsif value >= 126
        length = 0x7fff
        type = :BINARY
      elsif value <= 1
        length = 0
        type = :BINARY
      else
        length = value >> 1
        type = :FIXBINARY
      end
      nullable = (value & 1) == 0
      fields << { :type => type, :total_length => length, :nullable => nullable }
    end

    value = values.next
    if (value & 0x80) != 0
      value = (value & 0x7f) << 8 | values.next
    end

    @index_fields = { :fields => fields }
    if leaf?
      @index_fields[:trx_id_position] = value
    else
      @index_fields[:n_nullable] = value
    end

    @index_fields
  end

  def record_log_fields
    if record_format
      record_format.values_at(:key, :row).flatten.sort_by {|f| f.position}
    end
  end

  # Read records in the page modification log.
  def modification_log
    return @modification_log if @modification_log

    log_length = 0
    log_records = []
    cursor(pos_modification_log).name("modification_log") do |c|
      loop do
        value = c.get_uint8
        break unless value != 0
        if (value & 0x80) != 0
          value = (value & 0x7f) << 8 | c.get_uint8
        end
        heap_no = (value >> 1) + 1
        if (value & 1) != 0
          log_records << { :heap_no => heap_no }
          next
        end
        if record_fields
          record = read_record(c)
        else
          record = c.name("record") { c.get_bytes(record_length) }
        end
        log_records << { :heap_no => heap_no, :record => record }
      end
      log_length = c.position - pos_modification_log
    end

    @modification_log = { :length => log_length, :records => log_records }
  end

  # Read a given number of slots starting from a given slot index.
  def read_page_directory_slots(slot, num_slots)
    slots = []
    slot_pos = pos_directory - (slot * PAGE_ZIP_DIR_SLOT_SIZE)
    cursor(slot_pos).backward.name("page_directory") do |c|
      num_slots.times do |n|
        offset = c.name("slot[#{n}]") { c.get_uint16 }
        slots.push({
          :offset => offset & PAGE_ZIP_DIR_SLOT_MASK,
          :owned => (offset & PAGE_ZIP_DIR_SLOT_OWNED) != 0,
          :deleted => (offset & PAGE_ZIP_DIR_SLOT_DEL) != 0,
        })
      end
    end
    slots
  end

  # Return an array of row offsets for all entries in the page directory.
  def directory
    return @directory if @directory
    @directory = read_page_directory_slots(0, directory_slots)
  end

  # Return an array of row offsets for all entries in the free list of the
  # page directory.
  def directory_free
    return @directory_free if @directory_free
    @directory_free = read_page_directory_slots(directory_slots,
      directory_free_slots)
  end

  def record_length
    length = index_fields[:fields].inject(0) do |len, hash|
      len + hash[:total_length]
    end
    length -= SYS_FIELD_TRX_ID_LENGTH + SYS_FIELD_ROLL_PTR_LENGTH if leaf?
  end

  def read_record(cursor)
    record = []
    this_record = {
      :format => page_header[:format],
      :header => { :lengths => [], :nulls => [], :externs => [] },
    }
    record_log_fields.each do |f|
      cursor.name(f.name) do
        record << {
          :name => f.name,
          :type => f.data_type.name,
          :value => f.value(cursor, this_record),
        }
      end
    end
    record
  end

  def compressed_records
    no_heap = 2
    records = []
    cursor = BufferCursor.new(inflate_block[1], 0).forward
    directory_slots.times do |n|
      break if cursor.position == inflate_block[1].size
      record = record_fields ? read_record(cursor) : cursor.get_bytes(record_length)
      records << { :no_heap => no_heap + n, :record => record }
    end
    records
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
    puts "  %-17s%5i" % [ "header",           header_space ]
    puts "  %-17s%5i" % [ "trailer",          trailer_space ]
    puts "  %-17s%5i" % [ "directory",        directory_space ]
    puts "  %-17s%5i" % [ "compressed",       compressed_space ]
    puts "  %-17s%5i" % [ "modification log", modification_log_space ]
    puts "  %-17s%5i" % [ "free",             free_space ]
    puts "  %-17s%5i" % [ "used",             used_space ]
    puts "  %-17s%5i" % [ "record",           record_space ]
    puts "  %-17s%5.2f" % [
      "per record",
      (records > 0) ? (record_space / records) : 0
    ]
    puts

    puts "index fields:"
    pp index_fields
    puts

    puts "modification log:"
    pp modification_log
    puts

    puts "dense page directory:"
    pp directory
    puts

    puts "dense page directory free list:"
    pp directory_free
    puts

    puts "compressed records:"
    pp compressed_records

    puts
  end
end
