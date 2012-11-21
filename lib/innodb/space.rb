# An InnoDB tablespace file, which can be either a multi-table ibdataN file
# or a single-table "innodb_file_per_table" .ibd file.
class Innodb::Space
  # Open a tablespace file.
  def initialize(file)
    @file = File.open(file)
    @size = @file.stat.size
    @pages = (@size / Innodb::Page::PAGE_SIZE)
  end

  # Get an Innodb::Page object for a specific page by page number.
  def page(page_number, record_formatter=nil)
    offset = page_number.to_i * Innodb::Page::PAGE_SIZE
    return nil unless offset < @size
    return nil unless (offset + Innodb::Page::PAGE_SIZE) <= @size
    @file.seek(offset)
    page_data = @file.read(Innodb::Page::PAGE_SIZE)
    Innodb::Page.new(page_data, record_formatter)
  end

  # Iterate through all pages in a tablespace, returning the page number
  # and an Innodb::Page object for each one.
  def each_page
    (0...@pages).each do |page_number|
      current_page = page(page_number)
      yield page_number, current_page if current_page
    end
  end
end
