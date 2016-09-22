# -*- encoding : utf-8 -*-

# A generic class for any type of page, which handles reading the common
# FIL header and trailer, and can handle (via #parse) dispatching to a more
# specialized class depending on page type (which comes from the FIL header).
# A page being handled by Innodb::Page indicates that its type is not currently
# handled by any more specialized class.
class Innodb::Page
  # A hash of page types to specialized classes to handle them. Normally
  # subclasses will register themselves in this list.
  SPECIALIZED_CLASSES = {}

  # Load a page as a generic page in order to make the "fil" header accessible,
  # and then attempt to hand off the page to a specialized class to be
  # re-parsed if possible. If there is no specialized class for this type
  # of page, return the generic object.
  #
  # This could be optimized to reach into the page buffer and efficiently
  # extract the page type in order to avoid throwing away a generic
  # Innodb::Page object when parsing every specialized page, but this is
  # a bit cleaner, and we're not particularly performance sensitive.
  def self.parse(space, buffer, page_number=nil)
    # Create a page object as a generic page.
    page = Innodb::Page.new(space, buffer, page_number)

    # If there is a specialized class available for this page type, re-create
    # the page object using that specialized class.
    if specialized_class = SPECIALIZED_CLASSES[page.type]
      page = specialized_class.handle(page, space, buffer, page_number)
    end

    page
  end

  # Allow the specialized class to do something that isn't 'new' with this page.
  def self.handle(page, space, buffer, page_number=nil)
    self.new(space, buffer, page_number)
  end

  # Initialize a page by passing in a buffer containing the raw page contents.
  # The buffer size should match the space's page size.
  def initialize(space, buffer, page_number=nil)
    unless space && buffer
      raise "Page can't be initialized from nil space or buffer (space: #{space}, buffer: #{buffer})"
    end

    unless space.page_size == buffer.size
      raise "Buffer size #{buffer.size} is different than space page size"
    end

    @space  = space
    @buffer = buffer
    @page_number = page_number
  end

  attr_reader :space

  # Return the page size, to eventually be able to deal with non-16kB pages.
  def size
    @size ||= @buffer.size
  end

  # Return a simple string to uniquely identify this page within the space.
  # Be careful not to call anything which would instantiate a BufferCursor
  # so that we can use this method in cursor initialization.
  def name
    page_offset = BinData::Uint32be.read(@buffer.slice(4, 4))
    page_type = BinData::Uint16be.read(@buffer.slice(24, 2))
    "%i,%s" % [
      page_offset,
      PAGE_TYPE_BY_VALUE[page_type],
    ]
  end

  # If no block is passed, return an BufferCursor object positioned at a
  # specific offset. If a block is passed, create a cursor at the provided
  # offset and yield it to the provided block one time, and then return the
  # return value of the block.
  def cursor(buffer_offset)
    new_cursor = BufferCursor.new(@buffer, buffer_offset)
    new_cursor.push_name("space[#{space.name}]")
    new_cursor.push_name("page[#{name}]")

    if block_given?
      # Call the block once and return its return value.
      yield new_cursor
    else
      # Return the cursor itself.
      new_cursor
    end
  end

  # Return the byte offset of the start of the "fil" header, which is at the
  # beginning of the page. Included here primarily for completeness.
  def pos_fil_header
    0
  end

  # Return the size of the "fil" header, in bytes.
  def size_fil_header
    4 + 4 + 4 + 4 + 8 + 2 + 8 + 4
  end

  # The start of the checksummed portion of the file header.
  def pos_partial_page_header
    pos_fil_header + 4
  end

  # The size of the portion of the fil header that is included in the
  # checksum. Exclude the following:
  #   :checksum   (offset 4, size 4)
  #   :flush_lsn  (offset 26, size 8)
  #   :space_id   (offset 34, size 4)
  def size_partial_page_header
    size_fil_header - 4 - 8 - 4
  end

  # Return the byte offset of the start of the "fil" trailer, which is at
  # the end of the page.
  def pos_fil_trailer
    size - size_fil_trailer
  end

  # Return the size of the "fil" trailer, in bytes.
  def size_fil_trailer
    4 + 4
  end

  # Return the position of the "body" of the page, which starts after the FIL
  # header.
  def pos_page_body
    pos_fil_header + size_fil_header
  end

  # Return the size of the page body, excluding the header and trailer.
  def size_page_body
    size - size_fil_trailer - size_fil_header
  end

  # InnoDB Page Type constants from include/fil0fil.h.
  PAGE_TYPE = {
    :ALLOCATED => {
      :value => 0,
      :description => "Freshly allocated",
      :usage => "page type field has not been initialized",
    },
    :UNDO_LOG => {
      :value => 2,
      :description => "Undo log",
      :usage => "stores previous values of modified records",
    },
    :INODE => {
      :value => 3,
      :description => "File segment inode",
      :usage => "bookkeeping for file segments",
    },
    :IBUF_FREE_LIST => {
      :value => 4,
      :description => "Insert buffer free list",
      :usage => "bookkeeping for insert buffer free space management",
    },
    :IBUF_BITMAP => {
      :value => 5,
      :description => "Insert buffer bitmap",
      :usage => "bookkeeping for insert buffer writes to be merged",
    },
    :SYS => {
      :value => 6,
      :description => "System internal",
      :usage => "used for various purposes in the system tablespace",
    },
    :TRX_SYS => {
      :value => 7,
      :description => "Transaction system header",
      :usage => "bookkeeping for the transaction system in system tablespace",
    },
    :FSP_HDR => {
      :value => 8,
      :description => "File space header",
      :usage => "header page (page 0) for each tablespace file",
    },
    :XDES => {
      :value => 9,
      :description => "Extent descriptor",
      :usage => "header page for subsequent blocks of 16,384 pages",
    },
    :BLOB => {
      :value => 10,
      :description => "Uncompressed BLOB",
      :usage => "externally-stored uncompressed BLOB column data",
    },
    :ZBLOB => {
      :value => 11,
      :description => "First compressed BLOB",
      :usage => "externally-stored compressed BLOB column data, first page",
    },
    :ZBLOB2 => {
      :value => 12,
      :description => "Subsequent compressed BLOB",
      :usage => "externally-stored compressed BLOB column data, subsequent page",
    },
    :INDEX => {
      :value => 17855,
      :description => "B+Tree index",
      :usage => "table and index data stored in B+Tree structure",
    },
  }

  PAGE_TYPE_BY_VALUE = PAGE_TYPE.inject({}) { |h, (k, v)| h[v[:value]] = k; h }

  # A helper to convert "undefined" values stored in previous and next pointers
  # in the page header to nil.
  def self.maybe_undefined(value)
    value == 4294967295 ? nil : value
  end

  # Return the "fil" header from the page, which is common for all page types.
  def fil_header
    @fil_header ||= cursor(pos_fil_header).name("fil_header") do |c|
      {
        :checksum   => c.name("checksum") { c.get_uint32 },
        :offset     => c.name("offset") { c.get_uint32 },
        :prev       => c.name("prev") {
          Innodb::Page.maybe_undefined(c.get_uint32)
        },
        :next       => c.name("next") {
          Innodb::Page.maybe_undefined(c.get_uint32)
        },
        :lsn        => c.name("lsn") { c.get_uint64 },
        :type       => c.name("type") { PAGE_TYPE_BY_VALUE[c.get_uint16] },
        :flush_lsn  => c.name("flush_lsn") { c.get_uint64 },
        :space_id   => c.name("space_id") { c.get_uint32 },
      }
    end
  end

  # Return the "fil" trailer from the page, which is common for all page types.
  def fil_trailer
    @fil_trailer ||= cursor(pos_fil_trailer).name("fil_trailer") do |c|
      {
        :checksum  => c.name("checksum") { c.get_uint32 },
        :lsn_low32 => c.name("lsn_low32") { c.get_uint32 },
      }
    end
  end

  # A helper function to return the checksum from the "fil" header, for easier
  # access.
  def checksum
    fil_header[:checksum]
  end

  # A helper function to return the checksum from the "fil" trailer, for easier
  # access.
  def checksum_trailer
    fil_trailer[:checksum]
  end

  # A helper function to return the page offset from the "fil" header, for
  # easier access.
  def offset
    fil_header[:offset]
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

  # A helper function to return the LSN from the page header, for easier access.
  def lsn
    fil_header[:lsn]
  end

  # A helper function to return the low 32 bits of the LSN from the page header
  # for use in comparing to the low 32 bits stored in the trailer.
  def lsn_low32_header
    fil_header[:lsn] & 0xffffffff
  end

  # A helper function to return the low 32 bits of the LSN as stored in the page
  # trailer.
  def lsn_low32_trailer
    fil_trailer[:lsn_low32]
  end

  # A helper function to return the page type from the "fil" header, for easier
  # access.
  def type
    fil_header[:type]
  end

  # A helper function to return the space ID from the "fil" header, for easier
  # access.
  def space_id
    fil_header[:space_id]
  end

  # Iterate each byte of the FIL header.
  def each_page_header_byte_as_uint8
    unless block_given?
      return enum_for(:each_page_header_byte_as_uint8)
    end

    cursor(pos_partial_page_header).
      each_byte_as_uint8(size_partial_page_header) do |byte|
      yield byte
    end
  end

  # Iterate each byte of the page body, except for the FIL header and
  # the FIL trailer.
  def each_page_body_byte_as_uint8
    unless block_given?
      return enum_for(:each_page_body_byte_as_uint8)
    end

    cursor(pos_page_body).
      each_byte_as_uint8(size_page_body) do |byte|
      yield byte
    end
  end

  # Calculate the checksum of the page using InnoDB's algorithm.
  def checksum_innodb
    unless size == 16384
      raise "Checksum calculation is only supported for 16 KiB pages"
    end

    @checksum_innodb ||= begin
      # Calculate the InnoDB checksum of the page header.
      c_partial_header = Innodb::Checksum.fold_enumerator(each_page_header_byte_as_uint8)

      # Calculate the InnoDB checksum of the page body.
      c_page_body = Innodb::Checksum.fold_enumerator(each_page_body_byte_as_uint8)

      # Add the two checksums together, and mask the result back to 32 bits.
      (c_partial_header + c_page_body) & Innodb::Checksum::MAX
    end
  end

  def checksum_innodb?
    checksum == checksum_innodb
  end

  # Calculate the checksum of the page using the CRC32c algorithm.
  def checksum_crc32
    unless size == 16384
      raise "Checksum calculation is only supported for 16 KiB pages"
    end

    @checksum_crc32 ||= begin
      # Calculate the CRC32c of the page header.
      crc_partial_header = Digest::CRC32c.new
      each_page_header_byte_as_uint8 do |byte|
        crc_partial_header << byte.chr
      end

      # Calculate the CRC32c of the page body.
      crc_page_body = Digest::CRC32c.new
      each_page_body_byte_as_uint8 do |byte|
        crc_page_body << byte.chr
      end

      # Bitwise XOR the two checksums together.
      crc_partial_header.checksum ^ crc_page_body.checksum
    end
  end

  def checksum_crc32?
    checksum == checksum_crc32
  end

  # Is the page checksum correct?
  def checksum_valid?
    checksum_crc32? || checksum_innodb?
  end

  # Is the page checksum incorrect?
  def checksum_invalid?
    !checksum_valid?
  end

  def checksum_type
    case
    when checksum_crc32?
      :crc32
    when checksum_innodb?
      :innodb
    end
  end

  # Is the LSN stored in the header different from the one stored in the
  # trailer?
  def torn?
    lsn_low32_header != lsn_low32_trailer
  end

  # Is the page in the doublewrite buffer?
  def in_doublewrite_buffer?
    space && space.system_space? && space.doublewrite_page?(offset)
  end

  # Is the space ID stored in the header different from that of the space
  # provided when initializing this page?
  def misplaced_space?
    space && (space_id != space.space_id)
  end

  # Is the page number stored in the header different from the page number
  # which was supposed to be read?
  def misplaced_offset?
    offset != @page_number
  end

  # Is the page misplaced in the wrong file or by offset in the file?
  def misplaced?
    !in_doublewrite_buffer? && (misplaced_space? || misplaced_offset?)
  end

  # Is the page corrupt, either due to data corruption, tearing, or in the
  # wrong place?
  def corrupt?
    checksum_invalid? || torn? || misplaced?
  end

  def each_region
    unless block_given?
      return enum_for(:each_region)
    end

    yield({
      :offset => pos_fil_header,
      :length => size_fil_header,
      :name   => :fil_header,
      :info   => "FIL Header",
    })

    yield({
      :offset => pos_fil_trailer,
      :length => size_fil_trailer,
      :name   => :fil_trailer,
      :info   => "FIL Trailer",
    })

    nil
  end

  # Implement a custom inspect method to avoid irb printing the contents of
  # the page buffer, since it's very large and mostly not interesting.
  def inspect
    if fil_header
      "#<%s: size=%i, space_id=%i, offset=%i, type=%s, prev=%s, next=%s, checksum_valid?=%s (%s), torn?=%s, misplaced?=%s>" % [
        self.class,
        size,
        fil_header[:space_id],
        fil_header[:offset],
        fil_header[:type],
        fil_header[:prev] || "nil",
        fil_header[:next] || "nil",
        checksum_valid?,
        checksum_type ? checksum_type : "unknown",
        torn?,
        misplaced?,
      ]
    else
      "#<#{self.class}>"
    end
  end

  # Dump the contents of a page for debugging purposes.
  def dump
    puts "#{self}:"
    puts

    puts "fil header:"
    pp fil_header
    puts

    puts "fil trailer:"
    pp fil_trailer
    puts
  end
end
