require "innodb/cursor"

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
  def self.parse(space, buffer)
    # Create a page object as a generic page.
    page = Innodb::Page.new(space, buffer)

    # If there is a specialized class available for this page type, re-create
    # the page object using that specialized class.
    if specialized_class = SPECIALIZED_CLASSES[page.type]
      page = specialized_class.handle(page, space, buffer)
    end

    page
  end

  # Allow the specialized class to do something that isn't 'new' with this page.
  def self.handle(page, space, buffer)
    self.new(space, buffer)
  end

  # Initialize a page by passing in a buffer containing the raw page contents.
  # The buffer size should match the space's page size.
  def initialize(space, buffer)
    unless space && buffer
      raise "Page can't be initialized from nil space or buffer (space: #{space}, buffer: #{buffer})"
    end

    unless space.page_size == buffer.size
      raise "Buffer size #{buffer.size} is different than space page size"
    end

    @space  = space
    @buffer = buffer
  end

  attr_reader :space

  # Return the page size, to eventually be able to deal with non-16kB pages.
  def size
    @size ||= @buffer.size
  end

  # A helper function to return bytes from the page buffer based on offset
  # and length, both in bytes.
  def data(offset, length)
    @buffer[offset...(offset + length)]
  end

  # If no block is passed, return an Innodb::Cursor object positioned at a
  # specific offset. If a block is passed, create a cursor at the provided
  # offset and yield it to the provided block one time, and then return the
  # return value of the block.
  def cursor(offset)
    new_cursor = Innodb::Cursor.new(self, offset)

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

  # A helper to convert "undefined" values stored in previous and next pointers
  # in the page header to nil.
  def self.maybe_undefined(value)
    value == 4294967295 ? nil : value
  end

  # Return the "fil" header from the page, which is common for all page types.
  def fil_header
    @fil_header ||= cursor(pos_fil_header).name("fil") do |c|
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
        :type       => c.name("type") { PAGE_TYPE[c.get_uint16] },
        :flush_lsn  => c.name("flush_lsn") { c.get_uint64 },
        :space_id   => c.name("space_id") { c.get_uint32 },
      }
    end
  end

  # A helper function to return the checksum from the "fil" header, for easier
  # access.
  def checksum
    fil_header[:checksum]
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

  # A helper function to return the LSN, for easier access.
  def lsn
    fil_header[:lsn]
  end

  # A helper function to return the page type from the "fil" header, for easier
  # access.
  def type
    fil_header[:type]
  end

  # Calculate the checksum of the page using InnoDB's algorithm. Two sections
  # of the page are checksummed separately, and then added together to produce
  # the final checksum.
  def calculate_checksum
    unless size == 16384
      raise "Checksum calculation is only supported for 16 KiB pages"
    end

    # Calculate the checksum of the FIL header, except for the following:
    #   :checksum   (offset 4, size 4)
    #   :flush_lsn  (offset 26, size 8)
    #   :space_id   (offset 34, size 4)
    c_partial_header =
      Innodb::Checksum.fold_enumerator(
        cursor(pos_fil_header + 4).each_byte_as_uint8(
          size_fil_header - 4 - 8 - 4
        )
      )

    # Calculate the checksum of the page body, except for the FIL header and
    # the FIL trailer.
    c_page_body =
      Innodb::Checksum.fold_enumerator(
        cursor(pos_page_body).each_byte_as_uint8(
          size - size_fil_trailer - size_fil_header
        )
      )

    # Add the two checksums together, and mask the result back to 32 bits.
    (c_partial_header + c_page_body) & Innodb::Checksum::MAX
  end

  # Is the page corrupt? Calculate the checksum of the page and compare to
  # the stored checksum; return true or false.
  def corrupt?
    checksum != calculate_checksum
  end

  # Implement a custom inspect method to avoid irb printing the contents of
  # the page buffer, since it's very large and mostly not interesting.
  def inspect
    if fil_header
      "#<%s: size=%i, space_id=%i, offset=%i, type=%s, prev=%s, next=%s>" % [
        self.class,
        size,
        fil_header[:space_id],
        fil_header[:offset],
        fil_header[:type],
        fil_header[:prev] || "nil",
        fil_header[:next] || "nil",
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
  end
end
