# An InnoDB file segment entry, which appears in a few places, such as the
# FSEG header of INDEX pages, and in the TRX_SYS pages.
class Innodb::FsegEntry
  SIZE = 4 + 4 + 2

  # Return the FSEG entry address, which points to an entry on an INODE page.
  def self.get_entry_address(cursor)
    {
      :space_id     => cursor.get_uint32,
      :page_number  => cursor.get_uint32,
      :offset       => cursor.get_uint16,
    }
  end

  # Return an INODE entry which represents this file segment.
  def self.get_inode(space, cursor)
    address = get_entry_address(cursor)
    page = space.page(address[:page_number])
    if page.type == :INODE
      page.inode_at(address[:offset])
    end
  end
end