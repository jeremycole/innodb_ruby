require "innodb/free_list"

class Innodb::Page::Inode < Innodb::Page
  FRAG_ARRAY_N_SLOTS  = 32 # FSP_EXTENT_SIZE / 2
  FRAG_SLOT_SIZE      = 4

  MAGIC_N_VALUE	= 97937874

  def pos_inode_list
    pos_fil_header + size_fil_header + Innodb::FreeList::NODE_SIZE
  end

  def size_inode
    (16 + (3 * Innodb::FreeList::BASE_NODE_SIZE) +
      (FRAG_ARRAY_N_SLOTS * FRAG_SLOT_SIZE))
  end

  def inodes_per_page
    (size - pos_inode_list - 10) / size_inode
  end

  def uint32_array(size, cursor)
    size.times.map { |n| cursor.get_uint32 }
  end

  def inode(cursor)
    {
      :fseg_id            => cursor.get_uint64,
      :not_full_n_used    => cursor.get_uint32,
      :free               => Innodb::FreeList.get_base_node(cursor),
      :not_full           => Innodb::FreeList.get_base_node(cursor),
      :full               => Innodb::FreeList.get_base_node(cursor),
      :magic_n            => cursor.get_uint32,
      :frag_array         => uint32_array(FRAG_ARRAY_N_SLOTS, cursor),
    }
  end

  def each_inode
    inode_cursor = cursor(pos_inode_list)
    inodes_per_page.times do
      this_inode = inode(inode_cursor)
      yield this_inode if this_inode[:fseg_id] != 0
    end
  end

  def dump
    super

    puts "inodes:"
    each_inode do |i|
      pp i
    end
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:INODE] = Innodb::Page::Inode