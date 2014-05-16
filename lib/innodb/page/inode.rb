# -*- encoding : utf-8 -*-

require "innodb/list"

# A specialized class for handling INODE pages, which contain index FSEG (file
# segment) information. This allows all extents and individual pages assigned
# to each index to be found.
class Innodb::Page::Inode < Innodb::Page
  # Return the byte offset of the list node, which immediately follows the
  # FIL header.
  def pos_list_entry
    pos_page_body
  end

  # Return the size of the list node.
  def size_list_entry
    Innodb::List::NODE_SIZE
  end

  # Return the byte offset of the Inode array in the page, which immediately
  # follows the list entry.
  def pos_inode_array
    pos_list_entry + size_list_entry
  end

  # The number of Inode entries that fit on a page.
  def inodes_per_page
    (size - pos_inode_array - 10) / Innodb::Inode::SIZE
  end

  def size_inode_array
    inodes_per_page * Innodb::Inode::SIZE
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

  # Read a single Inode entry from the provided byte offset by creating a
  # cursor and reading the inode using the inode method.
  def inode_at(cursor)
    cursor.name("inode[#{cursor.position}]") { |c| Innodb::Inode.new_from_cursor(@space, c) }
  end

  # Iterate through all Inodes in the inode array.
  def each_inode
    unless block_given?
      return enum_for(:each_inode)
    end

    inode_cursor = cursor(pos_inode_array)
    inodes_per_page.times do |n|
      inode_cursor.name("inode[#{n}]") do |c|
        this_inode = Innodb::Inode.new_from_cursor(@space, c)
        yield this_inode
      end
    end
  end

  # Iterate through all allocated inodes in the inode array.
  def each_allocated_inode
    unless block_given?
      return enum_for(:each_allocated_inode)
    end

    each_inode do |this_inode|
      yield this_inode if this_inode.allocated?
    end
  end

  def each_region
    unless block_given?
      return enum_for(:each_region)
    end

    super do |region|
      yield region
    end

    yield({
      :offset => pos_list_entry,
      :length => size_list_entry,
      :name => :list_entry,
      :info => "Inode List Entry",
    })

    each_inode do |inode|
      if inode.allocated?
        yield({
          :offset => inode.offset,
          :length => Innodb::Inode::SIZE,
          :name => :inode_used,
          :info => "Inode (used)",
        })
      else
        yield({
          :offset => inode.offset,
          :length => Innodb::Inode::SIZE,
          :name => :inode_free,
          :info => "Inode (free)",
        })
      end
    end

    nil
  end

  # Dump the contents of a page for debugging purposes.
  def dump
    super

    puts "list entry:"
    pp list_entry
    puts

    puts "inodes:"
    each_inode do |inode|
      inode.dump
    end
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:INODE] = Innodb::Page::Inode
