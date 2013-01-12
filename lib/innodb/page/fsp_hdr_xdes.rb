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
  # A value added to the adjusted exponent stored in the page size field of
  # the flags in the FSP header.
  FLAGS_PAGE_SIZE_ADJUST = 9

  # Read a given number of bits from an integer at a specific bit offset. The
  # value returned is 0-based so does not need further shifting or adjustment.
  def self.read_bits_at_offset(data, bits, offset)
    ((data & (((1 << bits) - 1) << offset)) >> offset)
  end

  # Decode the "flags" field in the FSP header, returning a hash of useful
  # decoded flags. Unfortunately, InnoDB has a fairly weird and broken
  # implementation of these flags. The flags are:
  #
  # Offset    Size    Description
  # 0         1       Page Format (redundant, compact). This is unfortunately
  #                   coerced to 0 if it is "compact" and no other flags are
  #                   set, making it useless to innodb_ruby.
  # 1         4       Compressed Page Size (zip_size). This is stored as a
  #                   power of 2, minus 9. Since 0 is reserved to mean "not
  #                   compressed", the minimum value is 1, thus making the
  #                   smallest page size 1024 (2 ** (9 + 1)).
  # 5         1       Table Format (Antelope, Barracuda). This was supposed
  #                   to reserve 6 bits, but due to a bug in InnoDB only
  #                   actually reserved 1 bit.
  #
  def self.decode_flags(flags)
    # The page size for compressed pages is stored at bit offset 1 and consumes
    # 4 bits. Value 0 means the page is not compressed.
    page_size = read_bits_at_offset(flags, 4, 1)
    {
      :compressed => page_size == 0 ? false : true,
      :page_size => page_size == 0 ?
        Innodb::Space::DEFAULT_PAGE_SIZE :
        (1 << (FLAGS_PAGE_SIZE_ADJUST + page_size)),
      :value => flags,
    }
  end

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

  # The number of entries in the XDES array. Defined as page size divided by
  # extent size.
  def entries_in_xdes_array
    size / space.pages_per_extent
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
      :flags              => self.class.decode_flags(c.get_uint32),
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

  # Iterate through all lists in the file space.
  def each_list
    unless block_given?
      return enum_for(:each_list)
    end

    fsp_header.each do |key, value|
      yield key, value if value.is_a?(Innodb::List)
    end
  end

  # Iterate through all XDES entries in order. This is useful for debugging,
  # but each of these entries is actually a node in some other list. The state
  # field in the XDES entry indicates which type of list it is present in,
  # although not necessarily which list (e.g. :fseg).
  def each_xdes
    unless block_given?
      return enum_for(:each_xdes)
    end

    c = cursor(pos_xdes_array)
    entries_in_xdes_array.times do
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