# frozen_string_literal: true

# An InnoDB file segment entry, which appears in a few places, such as the
# FSEG header of INDEX pages, and in the TRX_SYS pages.

module Innodb
  class FsegEntry
    # The size (in bytes) of an FSEG entry, which contains a two 32-bit integers
    # and a 16-bit integer.
    SIZE = 4 + 4 + 2

    # Return the FSEG entry address, which points to an entry on an INODE page.
    def self.get_entry_address(cursor)
      {
        space_id: cursor.name('space_id') { cursor.read_uint32 },
        page_number: cursor.name('page_number') { Innodb::Page.maybe_undefined(cursor.read_uint32) },
        offset: cursor.name('offset') { cursor.read_uint16 },
      }
    end

    # Return an INODE entry which represents this file segment.
    def self.get_inode(space, cursor)
      address = cursor.name('address') { get_entry_address(cursor) }
      return nil if address[:offset].zero?

      page = space.page(address[:page_number])
      return nil unless page.type == :INODE

      page.inode_at(page.cursor(address[:offset]))
    end
  end
end
