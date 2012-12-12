require "innodb/list"
require "innodb/xdes"

class Innodb::Page::FspHdrXdes < Innodb::Page
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
      :free               => Innodb::List::Xdes.new(@space,
                              Innodb::List.get_base_node(c)),
      :free_frag          => Innodb::List::Xdes.new(@space,
                              Innodb::List.get_base_node(c)),
      :full_frag          => Innodb::List::Xdes.new(@space,
                              Innodb::List.get_base_node(c)),
      :first_unused_seg   => c.get_uint64,
      :full_inodes        => Innodb::List::Inode.new(@space,
                              Innodb::List.get_base_node(c)),
      :free_inodes        => Innodb::List::Inode.new(@space,
                              Innodb::List.get_base_node(c)),
    }
  end
  
  def each_xdes
    c = cursor(pos_xdes_array)
    XDES_N_ARRAY_ENTRIES.times do
      yield Innodb::Xdes.new(self, c)
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