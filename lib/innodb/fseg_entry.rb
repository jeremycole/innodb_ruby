# -*- encoding : utf-8 -*-

# An InnoDB file segment entry, which appears in a few places, such as the
# FSEG header of INDEX pages, and in the TRX_SYS pages.

class Innodb::FsegEntry
  # The size (in bytes) of an FSEG entry, which contains a two 32-bit integers
  # and a 16-bit integer.
  SIZE = 4 + 4 + 2

  # Return the FSEG entry address, which points to an entry on an INODE page.
  def self.get_entry_address(cursor)
    {
      :space_id     => cursor.name("space_id")    { cursor.get_uint32 },
      :page_number  => cursor.name("page_number") {
        Innodb::Page.maybe_undefined(cursor.get_uint32)
      },
      :offset       => cursor.name("offset")      { cursor.get_uint16 },
    }
  end

  # Return an INODE entry which represents this file segment.
  def self.get_inode(space, cursor)
    address = cursor.name("address") { get_entry_address(cursor) }
    if address[:offset] == 0
      return nil
    end

    page = space.page(address[:page_number])
    if page.type != :INODE
      return nil
    end

    page.inode_at(page.cursor(address[:offset]))
  end
end
