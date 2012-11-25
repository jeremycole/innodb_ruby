# An InnoDB tablespace file, which can be either a multi-table ibdataN file
# or a single-table "innodb_file_per_table" .ibd file.
class Innodb::Space
  attr_accessor :record_formatter

  # Open a tablespace file.
  def initialize(file)
    @file = File.open(file)
    @size = @file.stat.size
    @pages = (@size / Innodb::Page::PAGE_SIZE)
    @record_formatter = nil
  end

  # Get an Innodb::Page object for a specific page by page number.
  def page(page_number)
    offset = page_number.to_i * Innodb::Page::PAGE_SIZE
    return nil unless offset < @size
    return nil unless (offset + Innodb::Page::PAGE_SIZE) <= @size
    @file.seek(offset)
    page_data = @file.read(Innodb::Page::PAGE_SIZE)
    this_page = Innodb::Page.parse(page_data)

    if this_page.type == :INDEX
      this_page.record_formatter = @record_formatter
    end

    this_page
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
