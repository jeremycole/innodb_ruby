# -*- encoding : utf-8 -*-

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
  extend ReadBitsAtOffset

  # A value added to the adjusted exponent stored in the page size field of
  # the flags in the FSP header.
  FLAGS_PAGE_SIZE_SHIFT = 9

  def self.shift_page_size(page_size_shifted)
    if page_size_shifted != 0
      (1 << (FLAGS_PAGE_SIZE_SHIFT + page_size_shifted))
    end
  end

  # Decode the "flags" field in the FSP header, returning a hash of useful
  # decodings of the flags (based on MySQl 5.6 definitions). The flags are:
  #
  # Offset    Size    Description
  # 0         1       Post-Antelope Flag.
  # 1         4       Compressed Page Size (zip_size). This is stored as a
  #                   power of 2, minus 9. Since 0 is reserved to mean "not
  #                   compressed", the minimum value is 1, thus making the
  #                   smallest page size 1024 (2 ** (9 + 1)).
  # 5         1       Atomic Blobs Flag.
  # 6         4       System Page Size (innodb_page_size, UNIV_PAGE_SIZE).
  #                   The setting of the system page size when the tablespace
  #                   was created, stored in the same format as the compressed
  #                   page size above.
  # 10        1       Data Directory Flag.
  #
  def self.decode_flags(flags)
    system_page_size =
      shift_page_size(read_bits_at_offset(flags, 4, 6)) ||
      Innodb::Space::DEFAULT_PAGE_SIZE
    compressed_page_size = shift_page_size(read_bits_at_offset(flags, 4, 1))

    {
      :system_page_size => system_page_size,
      :compressed => compressed_page_size ? true : false,
      :page_size => compressed_page_size || system_page_size,
      :post_antelope => read_bits_at_offset(flags, 1, 0) == 1,
      :atomic_blobs => read_bits_at_offset(flags, 1, 5) == 1,
      :data_directory => read_bits_at_offset(flags, 1, 10) == 1,
      :value => flags,
    }
  end

  # The FSP header immediately follows the FIL header.
  def pos_fsp_header
    pos_page_body
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

  def size_xdes_entry
    @size_xdes_entry ||= Innodb::Xdes.new(self, cursor(pos_xdes_array)).size_entry
  end

  def size_xdes_array
    entries_in_xdes_array * size_xdes_entry
  end

  # Read the FSP (filespace) header, which contains a few counters and flags,
  # as well as list base nodes for each list maintained in the filespace.
  def fsp_header
    @fsp_header ||= cursor(pos_fsp_header).name("fsp") do |c|
      {
        :space_id           => c.name("space_id") { c.get_uint32 },
        :unused             => c.name("unused") { c.get_uint32 },
        :size               => c.name("size") { c.get_uint32 },
        :free_limit         => c.name("free_limit") { c.get_uint32 },
        :flags              => c.name("flags") {
          self.class.decode_flags(c.get_uint32)
        },
        :frag_n_used        => c.name("frag_n_used") { c.get_uint32 },
        :free               => c.name("list[free]") {
          Innodb::List::Xdes.new(@space, Innodb::List.get_base_node(c))
        },
        :free_frag          => c.name("list[free_frag]") {
          Innodb::List::Xdes.new(@space, Innodb::List.get_base_node(c))
        },
        :full_frag          => c.name("list[full_frag]") {
          Innodb::List::Xdes.new(@space, Innodb::List.get_base_node(c))
        },
        :first_unused_seg   => c.name("first_unused_seg") { c.get_uint64 },
        :full_inodes        => c.name("list[full_inodes]") {
          Innodb::List::Inode.new(@space, Innodb::List.get_base_node(c))
        },
        :free_inodes        => c.name("list[free_inodes]") {
          Innodb::List::Inode.new(@space, Innodb::List.get_base_node(c))
        },
      }
    end
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

    cursor(pos_xdes_array).name("xdes_array") do |c|
      entries_in_xdes_array.times do |n|
        yield Innodb::Xdes.new(self, c)
      end
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
      :offset => pos_fsp_header,
      :length => size_fsp_header,
      :name => :fsp_header,
      :info => "FSP Header",
    })

    each_xdes do |xdes|
      state = xdes.state || "unused"
      yield({
        :offset => xdes.offset,
        :length => size_xdes_entry,
        :name => "xdes_#{state}".to_sym,
        :info => "Extent Descriptor (#{state})",
      })
    end

    nil
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
