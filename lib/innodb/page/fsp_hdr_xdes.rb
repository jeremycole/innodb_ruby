require "innodb/list"

class Innodb::Page::FspHdrXdes < Innodb::Page
  XDES_BITS_PER_PAGE = 2
  XDES_BITMAP_SIZE = (64 * XDES_BITS_PER_PAGE) / 8
  XDES_SIZE = 8 + Innodb::List::NODE_SIZE + 4 + XDES_BITMAP_SIZE

  XDES_N_ARRAY_ENTRIES = 256

  def pos_fsp_header
    pos_fil_header + size_fil_header
  end

  def size_fsp_header
    (32 + 5 * Innodb::List::BASE_NODE_SIZE)
  end

  def pos_xdes_array
    pos_fsp_header + size_fsp_header
  end

  def fsp_header
    c = cursor(pos_fsp_header)
    @fsp_header ||= {
      :space_id           => c.get_uint32,
      :unused             => c.get_uint32,
      :size               => c.get_uint32,
      :free_limit         => c.get_uint32,
      :flags              => c.get_uint32,
      :frag_n_used        => c.get_uint32,
      :free               => Innodb::List.get_base_node(c),
      :free_frag          => Innodb::List.get_base_node(c),
      :full_frag          => Innodb::List.get_base_node(c),
      :first_unused_seg   => c.get_uint64,
      :full_inodes        => Innodb::List.get_base_node(c),
      :free_inodes        => Innodb::List.get_base_node(c),
    }
  end

  XDES_STATES = {
    1 => :free,
    2 => :free_frag,
    3 => :full_frag,
    4 => :fseg,
  }

  def read_xdes(cursor)
    {
      :fseg_id    => cursor.get_uint64,
      :position   => cursor.position,
      :list       => Innodb::List.get_node(cursor),
      :state      => XDES_STATES[cursor.get_uint32],
      :bitmap     => cursor.get_hex(XDES_BITMAP_SIZE),
    }
  end
  
  def each_xdes
    c = cursor(pos_xdes_array)
    XDES_N_ARRAY_ENTRIES.times do
      yield read_xdes(c)
    end
  end

  def dump
    super

    puts "fsp header:"
    pp fsp_header
    puts

    puts "xdes entries:"
    each_xdes do |xdes|
      pp xdes
    end
    puts
  end
end

Innodb::Page::SPECIALIZED_CLASSES[:FSP_HDR] = Innodb::Page::FspHdrXdes
Innodb::Page::SPECIALIZED_CLASSES[:XDES]    = Innodb::Page::FspHdrXdes