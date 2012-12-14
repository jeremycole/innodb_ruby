require "innodb/list"
require "innodb/xdes"

# A specialized class for FSP_HDR (filespace header) and XDES (extent
# descriptor) page types. Each tablespace always has an FSP_HDR page as
# its first page (page 0), and has repeating XDES pages every 16,384 pages
# after that (page 16384, 32768, ...). The FSP_HDR and XDES page structure
# is completely identical, with the exception that the FSP header structure
# is zero-filled on XDES pages, but populated on FSP_HDR pages.
#
# The basic structure of FSP_HDR and XDES pages is: FIL header, FSP header,
# an array of 256 XDES entries, empty (unused) space, and FIL trailer.
class Innodb::Page::FspHdrXdes < Innodb::Page
  # This is actually defined as page size divided by extent size, which is
  # 16384 / 64 = 256.
  XDES_N_ARRAY_ENTRIES = 256

  # The FSP header immediately follows the FIL header.
  def pos_fsp_header
    pos_fil_header + size_fil_header
  end

  # The FSP header contains six 32-bit integers, one 64-bit integer, and 5
  # list base nodes.
  def size_fsp_header
    ((4 * 6) + (1 * 8) + (5 * Innodb::List::BASE_NODE_SIZE))
  end

  # The XDES entry array immediately follows the FSP header.
  def pos_xdes_array
    pos_fsp_header + size_fsp_header
  end

  # Read the FSP (filespace) header, which contains a few counters and flags,
  # as well as list base nodes for each list maintained in the filespace.
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

  # Iterate through all XDES entries in order. This is useful for debugging,
  # but each of these entries is actually a node in some other list. The state
  # field in the XDES entry indicates which type of list it is present in,
  # although not necessarily which list (e.g. :fseg).
  def each_xdes
    unless block_given?
      return Enumerable::Enumerator.new(self, :each_xdes)
    end

    c = cursor(pos_xdes_array)
    XDES_N_ARRAY_ENTRIES.times do
      yield Innodb::Xdes.new(self, c)
    end
  end

  # Dump the contents of a page for debugging purposes.
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