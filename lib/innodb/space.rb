# An InnoDB tablespace file, which can be either a multi-table ibdataN file
# or a single-table "innodb_file_per_table" .ibd file.
class Innodb::Space
  # InnoDB's default page size is 16KiB.
  DEFAULT_PAGE_SIZE = 16384

  # Open a tablespace file, providing the page size to use. Pages that aren't
  # 16 KiB may not be supported well.
  def initialize(file, page_size=DEFAULT_PAGE_SIZE)
    @file = File.open(file)
    @page_size = page_size
    @size = @file.stat.size
    @pages = (@size / page_size)
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

  # The number of pages per extent.
  def pages_per_extent
    64
  end

  # The size (in bytes) of an extent.
  def extent_size
    page_size * pages_per_extent
  end

  # Get an Innodb::Page object for a specific page by page number.
  def page(page_number)
    offset = page_number.to_i * page_size
    return nil unless offset < @size
    return nil unless (offset + page_size) <= @size
    @file.seek(offset)
    page_data = @file.read(page_size)
    this_page = Innodb::Page.parse(self, page_data)

    if this_page.type == :INDEX
      this_page.record_describer = @record_describer
    end

    this_page
  end

  # Get an Innodb::List object for a specific list by list name.
  def list(name)
    page(0).fsp_header[name]
  end

  # Get an Innodb::Index object for a specific index by root page number.
  def index(root_page_number)
    Innodb::Index.new(self, root_page_number)
  end

  # Iterate through each index by guessing that the root pages will be
  # present starting at page 3, and walking forward until we find a non-
  # root page. This should work fine for IBD files, but not for ibdata
  # files.
  def each_index
    unless block_given?
      return enum_for(:each_index)
    end

    (3...@pages).each do |page_number|
      page = page(page_number)
      if page.type == :INDEX && page.root?
        yield index(page_number)
      else
        break
      end
    end
  end

  # Iterate through all pages in a tablespace, returning the page number
  # and an Innodb::Page object for each one.
  def each_page(start_page=0)
    unless block_given?
      return enum_for(:each_page)
    end

    (start_page...@pages).each do |page_number|
      current_page = page(page_number)
      yield page_number, current_page if current_page
    end
  end

  # Iterate through unique regions in the space by page type. This is useful
  # to achieve an overall view of the space.
  def each_page_type_region(start_page=0)
    unless block_given?
      return enum_for(:each_page_type_region)
    end

    region = nil
    each_page(start_page) do |page_number, page|
      if region && region[:type] == page.type
        region[:end] = page_number
        region[:count] += 1
      else
        yield region if region
        region = {
          :start => page_number,
          :end   => page_number,
          :type  => page.type,
          :count => 1,
        }
      end
    end
    yield region if region
  end
end
