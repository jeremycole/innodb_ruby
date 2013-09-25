# -*- encoding : utf-8 -*-
# An InnoDB space file, which can be either a multi-table ibdataN file
# or a single-table "innodb_file_per_table" .ibd file.
class Innodb::Space
  # InnoDB's default page size is 16KiB.
  DEFAULT_PAGE_SIZE = 16384

  # A map of InnoDB system space fixed-allocation pages. This can be used to
  # check whether a space is a system space or not, as non-system spaces will
  # not match this pattern.
  SYSTEM_SPACE_PAGE_MAP = {
    0 => :FSP_HDR,
    1 => :IBUF_BITMAP,
    2 => :INODE,
    3 => :SYS,
    4 => :INDEX,
    5 => :TRX_SYS,
    6 => :SYS,
    7 => :SYS,
  }

  # Open a space file, optionally providing the page size to use. Pages
  # that aren't 16 KiB may not be supported well.
  def initialize(file, page_size=nil)
    @file = File.open(file)
    @size = @file.stat.size

    if page_size
      @page_size = page_size
    else
      @page_size = fsp_flags[:page_size]
    end

    @pages = (@size / @page_size)
    @compressed = fsp_flags[:compressed]
    @record_describer = nil
  end

  # An object which can be used to describe records found in pages within
  # this space.
  attr_accessor :record_describer

  # The size (in bytes) of each page in the space.
  attr_reader :page_size

  # The size (in bytes) of the space
  attr_reader :size

  # The number of pages in the space.
  attr_reader :pages

  # Read the FSP header "flags" field by byte offset within the space file.
  # This is useful in order to initialize the page size, as we can't properly
  # read the FSP_HDR page before we know its size.
  def raw_fsp_header_flags
    # A simple sanity check. The FIL header should be initialized in page 0,
    # to offset 0 and page type :FSP_HDR (8).
    page_offset = BinData::Uint32be.read(read_at_offset(4, 4))
    page_type   = BinData::Uint16be.read(read_at_offset(24, 2))
    unless page_offset == 0 && Innodb::Page::PAGE_TYPE_BY_VALUE[page_type] == :FSP_HDR
      raise "Something is very wrong; Page 0 does not seem to be type FSP_HDR"
    end

    # Another sanity check. The Space ID should be the same in both the FIL
    # and FSP headers.
    fil_space = BinData::Uint32be.read(read_at_offset(34, 4))
    fsp_space = BinData::Uint32be.read(read_at_offset(38, 4))
    unless fil_space == fsp_space
      raise "Something is very wrong; FIL and FSP header Space IDs don't match"
    end

    # Well, we're as sure as we can be. Read the flags field and decode it.
    flags_value = BinData::Uint32be.read(read_at_offset(54, 4))
    Innodb::Page::FspHdrXdes.decode_flags(flags_value)
  end

  # The FSP header flags, decoded. If the page size has not been initialized,
  # reach into the raw bytes of the FSP_HDR page and attempt to decode the
  # flags field that way.
  def fsp_flags
    if @page_size
      return fsp[:flags]
    else
      raw_fsp_header_flags
    end
  end

  # The size (in bytes) of an extent.
  def extent_size
    1048576
  end

  # The number of pages per extent.
  def pages_per_extent
    extent_size / page_size
  end

  # The number of pages per FSP_HDR/XDES page. This is crudely mapped to the
  # page size, and works for pages down to 1KiB.
  def pages_per_xdes_page
    page_size
  end

  # An array of all FSP/XDES page numbers for the space.
  def xdes_page_numbers
    (0..(@pages / pages_per_xdes_page)).map { |n| n * pages_per_xdes_page }
  end

  # The FSP_HDR/XDES page which will contain the XDES entry for a given page.
  def xdes_page_for_page(page_number)
    page_number - (page_number % pages_per_xdes_page)
  end

  # The XDES entry offset for a given page within its FSP_HDR/XDES page's
  # XDES array.
  def xdes_entry_for_page(page_number)
    relative_page_number = page_number - xdes_page_for_page(page_number)
    relative_page_number / pages_per_extent
  end

  # Return the Innodb::Xdes entry which represents a given page.
  def xdes_for_page(page_number)
    xdes_array = page(xdes_page_for_page(page_number)).each_xdes.to_a
    xdes_array[xdes_entry_for_page(page_number)]
  end

  # Get the raw byte buffer of size bytes at offset in the file.
  def read_at_offset(offset, size)
    @file.seek(offset)
    @file.read(size)
  end

  # Get the raw byte buffer for a specific page by page number.
  def page_data(page_number)
    offset = page_number.to_i * page_size
    return nil unless offset < @size
    return nil unless (offset + page_size) <= @size
    read_at_offset(offset, page_size)
  end

  # Get an Innodb::Page object for a specific page by page number.
  def page(page_number)
    this_page = Innodb::Page.parse(self, page_data(page_number))

    if this_page.type == :INDEX
      this_page.record_describer = @record_describer
    end

    this_page
  end

  # Determine whether this space looks like a system space. If the initial
  # pages in the space match the SYSTEM_SPACE_PAGE_MAP, it is likely to be
  # a system space.
  def system_space?
    SYSTEM_SPACE_PAGE_MAP.each do |page_number, type|
      # We can't use page() here, because system_space? need to be used
      # in the Innodb::Page::Sys.parse to determine what type of page
      # is being looked at. Using page() would cause us to keep recurse
      # infinitely. Use Innodb::Page.new instead to load the page as
      # simply as possible.
      test_page = Innodb::Page.new(self, page_data(page_number))
      return false unless test_page.type == type
    end
    true
  end

  # Return the page number for the space's FSP_HDR page.
  def page_fsp_hdr
    0
  end

  # Get (and cache) the FSP header from the FSP_HDR page.
  def fsp
    @fsp ||= page(page_fsp_hdr).fsp_header
  end

  def space_id
    fsp[:space_id]
  end

  # Return the page number for the space's TRX_SYS page.
  def page_trx_sys
    5
  end

  # Get the Innodb::Page::TrxSys page for a system space.
  def trx_sys
    page(page_trx_sys) if system_space?
  end

  def rseg_page?(page_number)
    if trx_sys
      trx_sys.rsegs.include?({
        :space_id => 0,
        :page_number => page_number,
      })
    end
  end

  # Return the page number for the space's SYS data dictionary header.
  def page_sys_data_dictionary
    7
  end

  # Get the Innodb::Page::SysDataDictionaryHeader page for a system space.
  def data_dictionary
    page(page_sys_data_dictionary) if system_space?
  end

  # Get an Innodb::List object for a specific list by list name.
  def list(name)
    if xdes_lists.include?(name) || inode_lists.include?(name)
      fsp[name]
    end
  end

  # Get an Innodb::Index object for a specific index by root page number.
  def index(root_page_number, record_describer=nil)
    Innodb::Index.new(self, root_page_number, record_describer || @record_describer)
  end

  # Iterate through each index by guessing that the root pages will be
  # present starting at page 3, and walking forward until we find a non-
  # root page. This should work fine for IBD files, but not for ibdata
  # files.
  def each_index
    unless block_given?
      return enum_for(:each_index)
    end

    if system_space?
      data_dictionary.each_index do |table_name, index_name, index|
        yield index
      end
    else
      (3...@pages).each do |page_number|
        page = page(page_number)
        if page.type == :INDEX && page.root?
          yield index(page_number)
        else
          break
        end
      end
    end
  end

  # An array of Innodb::Inode list names.
  def inode_lists
    [:full_inodes, :free_inodes]
  end

  # Iterate through Innodb::Inode lists in the space.
  def each_inode_list
    unless block_given?
      return enum_for(:each_inode_list)
    end

    inode_lists.each do |name|
      yield name, list(name)
    end
  end

  # Iterate through Innodb::Inode objects in the space.
  def each_inode
    unless block_given?
      return enum_for(:each_inode)
    end

    each_inode_list.each do |name, list|
      list.each do |page|
        page.each_inode do |inode|
          yield inode
        end
      end
    end
  end

  # Iterate through all pages in a space, returning the page number and an
  # Innodb::Page object for each one.
  def each_page(start_page=0)
    unless block_given?
      return enum_for(:each_page, start_page)
    end

    (start_page...@pages).each do |page_number|
      current_page = page(page_number)
      yield page_number, current_page if current_page
    end
  end

  # An array of Innodb::Xdes list names. 
  def xdes_lists
    [:free, :free_frag, :full_frag]
  end

  # Iterate through Innodb::Xdes lists in the space.
  def each_xdes_list
    unless block_given?
      return enum_for(:each_xdes_list)
    end

    xdes_lists.each do |name|
      yield name, list(name)
    end
  end

  # Iterate through all FSP_HDR/XDES pages, returning an Innodb::Page object
  # for each one.
  def each_xdes_page
    unless block_given?
      return enum_for(:each_xdes_page)
    end

    xdes_page_numbers.each do |page_number|
      current_page = page(page_number)
      yield current_page if current_page
    end
  end

  # Iterate through all extent descriptors for the space, returning an
  # Innodb::Xdes object for each one.
  def each_xdes
    unless block_given?
      return enum_for(:each_xdes)
    end

    each_xdes_page do |xdes_page|
      xdes_page.each_xdes do |xdes|
        # Only return initialized XDES entries; :state will be nil for extents
        # that have not been allocated yet.
        yield xdes if xdes.xdes[:state]
      end
    end
  end

  def each_page_status(start_page=0)
    unless block_given?
      return enum_for(:each_page_with_status, start_page)
    end

    each_xdes do |xdes|
      xdes.each_page_status do |page_number, page_status|
        next if page_number < start_page
        next if page_number >= @pages

        if this_page = page(page_number)
          yield page_number, this_page, page_status
        end
      end
    end
  end

  def type_for_page(page, page_status)
    page_status[:free] ? "FREE (#{page.type})" : page.type
  end

  # Iterate through unique regions in the space by page type. This is useful
  # to achieve an overall view of the space.
  def each_page_type_region(start_page=0)
    unless block_given?
      return enum_for(:each_page_type_region, start_page)
    end

    region = nil
    each_page_status(start_page) do |page_number, page, page_status|
      page_type = type_for_page(page, page_status)
      if region && region[:type] == page_type
        region[:end] = page_number
        region[:count] += 1
      else
        yield region if region
        region = {
          :start => page_number,
          :end   => page_number,
          :type  => page_type,
          :count => 1,
        }
      end
    end
    yield region if region
  end
end
