require "innodb/list"

# A specialized class for handling INODE pages, which contain index FSEG (file
# segment) information. This allows all extents and individual pages assigned
# to each index to be found.
class Innodb::Page::Inode < Innodb::Page
  # The number of "slots" (each representing one page) in the fragment array
  # within each Inode entry.
  FRAG_ARRAY_N_SLOTS  = 32 # FSP_EXTENT_SIZE / 2

  # The size (in bytes) of each slot in the fragment array.
  FRAG_SLOT_SIZE      = 4

  # A magic number which helps determine if an Inode structure is in use
  # and populated with valid data.
  MAGIC_N_VALUE	= 97937874

  # Return the byte offset of the list node, which immediately follows the
  # FIL header.
  def pos_list_entry
    pos_fil_header + size_fil_header
  end

  # Return the byte offset of the list node.
  def size_list_entry
    Innodb::List::NODE_SIZE
  end

  # Return the byte offset of the Inode array in the page, which immediately
  # follows the list entry.
  def pos_inode_array
    pos_list_entry + size_list_entry
  end

  # The size (in bytes) of an Inode entry.
  def size_inode
    (16 + (3 * Innodb::List::BASE_NODE_SIZE) +
      (FRAG_ARRAY_N_SLOTS * FRAG_SLOT_SIZE))
  end

  # The number of Inode entries that fit on a page.
  def inodes_per_page
    (size - pos_inode_array - 10) / size_inode
  end

  # Return the list entry.
  def list_entry
    cursor(pos_list_entry).name("list") do |c|
      Innodb::List.get_node(c)
    end
  end

  # Return the "previous" address pointer from the list entry. This is used
  # by Innodb::List::Inode to iterate through Inode lists.
  def prev_address
    list_entry[:prev]
  end

  # Return the "next" address pointer from the list entry. This is used
  # by Innodb::List::Inode to iterate through Inode lists.
  def next_address
    list_entry[:next]
  end

  # Read an array of page numbers (32-bit integers, which may be nil) from
  # the provided cursor.
  def page_number_array(size, cursor)
    size.times.map do |n|
      cursor.name("page[#{n}]") do |c|
        Innodb::Page.maybe_undefined(c.get_uint32)
      end
    end
  end

  # Read a single Inode entry from the provided cursor.
  def inode(cursor)
    {
      :fseg_id            => cursor.name("fseg_id") { cursor.get_uint64 },
      :not_full_n_used    => cursor.name("not_full_n_used") { cursor.get_uint32 },
      :free               => cursor.name("list[free]") { 
        Innodb::List::Xdes.new(@space, Innodb::List.get_base_node(cursor))
      },
      :not_full           => cursor.name("list[not_full]") { 
        Innodb::List::Xdes.new(@space, Innodb::List.get_base_node(cursor))
      },
      :full               => cursor.name("list[full]") { 
        Innodb::List::Xdes.new(@space, Innodb::List.get_base_node(cursor))
      },
      :magic_n            => cursor.name("magic_n") { cursor.get_uint32 },
      :frag_array         => cursor.name("frag_array") { 
        page_number_array(FRAG_ARRAY_N_SLOTS, cursor)
      },
    }
  end

  # Read a single Inode entry from the provided byte offset by creating a
  # cursor and reading the inode using the inode method.
  def inode_at(cursor)
    cursor.name("inode") { |c| inode(c) }
  end

  # Iterate through all Inodes in the inode array.
  def each_inode
    unless block_given?
      return enum_for(:each_inode)
    end

    inode_cursor = cursor(pos_inode_array)
    inodes_per_page.times do |n|
      inode_cursor.name("inode[#{n}]") do |c|
        this_inode = inode(c)
        yield this_inode if this_inode[:fseg_id] != 0
      end
    end
  end

  # Dump the contents of a page for debugging purposes.
  def dump
    super

    puts "list entry:"
    pp list_entry
    puts

    puts "inodes:"
    each_inode do |i|
      pp i
    end
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:INODE] = Innodb::Page::Inode