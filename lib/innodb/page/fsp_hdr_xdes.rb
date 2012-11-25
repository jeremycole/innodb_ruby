require "innodb/free_list"

class Innodb::Page::FspHdrXdes < Innodb::Page
  FSP_HEADER_SIZE   = (32 + 5 * Innodb::FreeList::BASE_NODE_SIZE)
  FSP_HEADER_START  = Innodb::Page::FIL_HEADER_END

  XDES_STATES = {
    1 => :free,
    2 => :free_frag,
    3 => :full_frag,
    4 => :fseg,
  }

  XDES_BITS_PER_PAGE = 2
  XDES_BITMAP_SIZE = (64 * XDES_BITS_PER_PAGE) / 8
  XDES_SIZE = 8 + Innodb::FreeList::NODE_SIZE + 4 + XDES_BITMAP_SIZE

  XDES_ARRAY_START      = FSP_HEADER_START + FSP_HEADER_SIZE
  XDES_N_ARRAY_ENTRIES  = 10

  def fsp_header
    c = cursor(FSP_HEADER_START)
    @fsp_header ||= {
      :space_id           => c.get_uint32,
      :unused             => c.get_uint32,
      :size               => c.get_uint32,
      :free_limit         => c.get_uint32,
      :flags              => c.get_uint32,
      :frag_n_used        => c.get_uint32,
      :free_frag          => Innodb::FreeList::get_base_node(c),
      :full_frag          => Innodb::FreeList::get_base_node(c),
      :first_unused_seg   => c.get_uint64,
      :full_inodes        => Innodb::FreeList::get_base_node(c),
      :free_inodes        => Innodb::FreeList::get_base_node(c),
    }
  end

  def read_xdes(cursor)
    {
      :xdes_id    => cursor.get_uint64,
      :free_list  => Innodb::FreeList::get_node(cursor),
      :state      => XDES_STATES[cursor.get_uint32],
      :bitmap     => cursor.get_bytes(XDES_BITMAP_SIZE),
    }
  end
  
  def each_xdes
    c = cursor(XDES_ARRAY_START)
    XDES_N_ARRAY_ENTRIES.times do
      yield read_xdes(c)
    end
  end

  def dump
    super

    puts
    puts "fsp header:"
    pp fsp_header
    
    puts
    puts "xdes entries:"
    each_xdes do |xdes|
      pp xdes
    end
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:FSP_HDR] = Innodb::Page::FspHdrXdes
Innodb::Page::SPECIALIZED_CLASSES[:XDES]    = Innodb::Page::FspHdrXdes