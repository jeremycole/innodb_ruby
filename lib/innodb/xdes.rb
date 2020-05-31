# frozen_string_literal: true

require 'forwardable'

# An InnoDB "extent descriptor entry" or "+XDES+". These structures are used
# in the +XDES+ entry array contained in +FSP_HDR+ and +XDES+ pages.
#
# Note the distinction between +XDES+ _entries_ and +XDES+ _pages_.
module Innodb
  class Xdes
    # An XDES entry structure.
    Entry = Struct.new(:offset, :start_page, :end_page, :fseg_id, :this, :list, :state, :bitmap, keyword_init: true)

    PageStatus = Struct.new(:free, :clean, keyword_init: true)

    # Number of bits per page in the +XDES+ entry bitmap field. Currently
    # +XDES+ entries store two bits per page, with the following meanings:
    #
    # * 1 = free (the page is free, or not in use)
    # * 2 = clean (currently unused, always 1 when initialized)
    BITS_PER_PAGE = 2

    # The bit value for a free page.
    BITMAP_BV_FREE  = 1

    # The bit value for a clean page (currently unused in InnoDB).
    BITMAP_BV_CLEAN = 2

    # The bitwise-OR of all bitmap bit values.
    BITMAP_BV_ALL = (BITMAP_BV_FREE | BITMAP_BV_CLEAN)

    # The values used in the +:state+ field indicating what the extent is
    # used for (or what list it is on).
    STATES = {
      # The extent is completely empty and unused, and should be present on the filespace's FREE list.
      1 => :free,

      # Some pages of the extent are used individually, and the extent should be present on the filespace's
      # FREE_FRAG list.
      2 => :free_frag,

      # All pages of the extent are used individually, and the extent should be present on the filespace's
      # FULL_FRAG list.
      3 => :full_frag,

      # The extent is wholly allocated to a file segment. Additional information about the state of this extent can
      # be derived from the its presence on particular file segment lists (FULL, NOT_FULL, or FREE).
      4 => :fseg,
    }.freeze

    attr_reader :xdes

    extend Forwardable

    def_delegator :xdes, :offset
    def_delegator :xdes, :start_page
    def_delegator :xdes, :end_page
    def_delegator :xdes, :fseg_id
    def_delegator :xdes, :this
    def_delegator :xdes, :list
    def_delegator :xdes, :state
    def_delegator :xdes, :bitmap

    def initialize(page, cursor)
      @page = page
      @xdes = read_xdes_entry(page, cursor)
    end

    # Size (in bytes) of the bitmap field in the +XDES+ entry.
    def size_bitmap
      (@page.space.pages_per_extent * BITS_PER_PAGE) / 8
    end

    # Size (in bytes) of the an +XDES+ entry.
    def size_entry
      8 + Innodb::List::NODE_SIZE + 4 + size_bitmap
    end

    # Read an XDES entry from a cursor.
    def read_xdes_entry(page, cursor)
      extent_number = (cursor.position - page.pos_xdes_array) / size_entry
      start_page = page.offset + (extent_number * page.space.pages_per_extent)
      cursor.name("xdes[#{extent_number}]") do |c|
        Entry.new(
          offset: c.position,
          start_page: start_page,
          end_page: start_page + page.space.pages_per_extent - 1,
          fseg_id: c.name('fseg_id') { c.read_uint64 },
          this: Innodb::Page::Address.new(page: page.offset, offset: c.position),
          list: c.name('list') { Innodb::List.get_node(c) },
          state: c.name('state') { STATES[c.read_uint32] },
          bitmap: c.name('bitmap') { c.read_bytes(size_bitmap) }
        )
      end
    end

    # Return whether this XDES entry is allocated to an fseg (the whole extent
    # then belongs to the fseg).
    def allocated_to_fseg?
      fseg_id != 0
    end

    # Return the status for a given page. This is relatively inefficient as
    # implemented and could be done better.
    def page_status(page_number)
      page_status_array = each_page_status.to_a
      page_status_array[page_number - start_page][1]
    end

    # Iterate through all pages represented by this extent descriptor,
    # yielding a page status hash for each page, containing the following
    # fields:
    #
    #   :page   The page number.
    #   :free   Boolean indicating whether the page is free.
    #   :clean  Boolean indicating whether the page is clean (currently
    #           this bit is unused by InnoDB, and always set true).
    def each_page_status
      return enum_for(:each_page_status) unless block_given?

      bitmap.each_byte.each_with_index do |byte, byte_index|
        (0..3).each do |page_offset|
          page_number = start_page + (byte_index * 4) + page_offset
          page_bits = ((byte >> (page_offset * BITS_PER_PAGE)) & BITMAP_BV_ALL)
          page_status = PageStatus.new(
            free: (page_bits & BITMAP_BV_FREE != 0),
            clean: (page_bits & BITMAP_BV_CLEAN != 0)
          )
          yield page_number, page_status
        end
      end

      nil
    end

    # Return the count of free pages (free bit is true) on this extent.
    def free_pages
      each_page_status.inject(0) do |sum, (_page_number, page_status)|
        sum += 1 if page_status.free
        sum
      end
    end

    # Return the count of used pages (free bit is false) on this extent.
    def used_pages
      @page.space.pages_per_extent - free_pages
    end

    # Return the address of the previous list pointer from the list node
    # contained within the XDES entry. This is used by +Innodb::List::Xdes+
    # to iterate through XDES entries in a list.
    def prev_address
      list.prev
    end

    # Return the address of the next list pointer from the list node
    # contained within the XDES entry. This is used by +Innodb::List::Xdes+
    # to iterate through XDES entries in a list.
    def next_address
      list.next
    end

    # Compare one Innodb::Xdes to another.
    def ==(other)
      this.page == other.this.page && this.offset == other.this.offset
    end
  end
end
