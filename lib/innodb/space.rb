# An InnoDB tablespace file, which can be either a multi-table ibdataN file
# or a single-table "innodb_file_per_table" .ibd file.
class Innodb::Space
  attr_accessor :record_describer
  attr_reader :pages

  # Currently only 16kB InnoDB pages are supported.
  PAGE_SIZE = 16384

  # Open a tablespace file.
  def initialize(file)
    @file = File.open(file)
    @size = @file.stat.size
    @pages = (@size / PAGE_SIZE)
    @record_describer = nil
  end

  # Get an Innodb::Page object for a specific page by page number.
  def page(page_number)
    offset = page_number.to_i * PAGE_SIZE
    return nil unless offset < @size
    return nil unless (offset + PAGE_SIZE) <= @size
    @file.seek(offset)
    page_data = @file.read(PAGE_SIZE)
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
    (3...@pages).each do |page_number|
      if page(page_number).root?
        yield index(page_number)
      else
        break
      end
    end
  end

  # Iterate through all pages in a tablespace, returning the page number
  # and an Innodb::Page object for each one.
  def each_page
    (0...@pages).each do |page_number|
      current_page = page(page_number)
      yield page_number, current_page if current_page
    end
  end

  # Iterate through unique regions in the space by page type. This is useful
  # to achieve an overall view of the space.
  def each_page_type_region
    region = nil
    each_page do |page_number, page|
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
