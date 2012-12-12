require "innodb/cursor"

class Innodb::Page
  SPECIALIZED_CLASSES = {}

  # Load a page as a generic page in order to make the "fil" header accessible,
  # and then attempt to hand off the page to a specialized class to be
  # re-parsed if possible. If there is no specialized class for this type
  # of page, return the generic object.
  def self.parse(buffer)
    page = Innodb::Page.new(buffer)

    if specialized_class = SPECIALIZED_CLASSES[page.type]
      page = specialized_class.new(buffer)
    end

    page
  end

  # Initialize a page by passing in a 16kB buffer containing the raw page
  # contents. Currently only 16kB pages are supported.
  def initialize(buffer)
    @buffer = buffer
  end

  # Return the page size, to eventually be able to deal with non-16kB pages.
  def size
    @size ||= @buffer.size
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

  # Return the byte offset of the start of the "fil" header, which is at the
  # beginning of the page. Included here primarily for completeness.
  def pos_fil_header
    0
  end

  # Return the size of the "fil" header, in bytes.
  def size_fil_header
    38
  end

  # Return the byte offset of the start of the "fil" trailer, which is at
  # the end of the page.
  def pos_fil_trailer
    size - size_fil_trailer
  end

  # Return the size of the "fil" trailer, in bytes.
  def size_fil_trailer
    8
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
    c = cursor(pos_fil_header)
    @fil_header ||= {
      :checksum   => c.get_uint32,
      :offset     => c.get_uint32,
      :prev       => Innodb::Page.maybe_undefined(c.get_uint32),
      :next       => Innodb::Page.maybe_undefined(c.get_uint32),
      :lsn        => c.get_uint64,
      :type       => PAGE_TYPE[c.get_uint16],
      :flush_lsn  => c.get_uint64,
      :space_id   => c.get_uint32,
    }
  end

  # A helper function to return the page type from the "fil" header, for easier
  # access.
  def type
    fil_header[:type]
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

  def inspect
    if fil_header
      "#<%s: size=%i, space_id=%i, offset=%i, type=%s, prev=%i, next=%i>" % [
        self.class,
        size,
        fil_header[:space_id],
        fil_header[:offset],
        fil_header[:type],
        fil_header[:prev],
        fil_header[:next],
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
