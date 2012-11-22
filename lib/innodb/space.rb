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
  def page(page_number, record_formatter=nil)
    offset = page_number.to_i * Innodb::Page::PAGE_SIZE
    return nil unless offset < @size
    return nil unless (offset + Innodb::Page::PAGE_SIZE) <= @size
    @file.seek(offset)
    page_data = @file.read(Innodb::Page::PAGE_SIZE)
    Innodb::Page.new(page_data, record_formatter || @record_formatter)
  end

  # Iterate through all pages in a tablespace, returning the page number
  # and an Innodb::Page object for each one.
  def each_page
    (0...@pages).each do |page_number|
      current_page = page(page_number)
      yield page_number, current_page if current_page
    end
  end

  def _recurse_index(page, node_proc, leaf_proc, link_proc, depth=0)
    if page.level == 0
      leaf_proc.call(page, depth) if page.type == :INDEX
    else
      node_proc.call(page, depth) if page.type == :INDEX
    end
    page.each_child_page do |child_page_number, child_min_key|
      child_page = page(child_page_number)
      if child_page.type == :INDEX
        link_proc.call(page, child_page, child_min_key, depth+1)
        _recurse_index(child_page, node_proc, leaf_proc, link_proc, depth+1)
      end
    end
  end

  def recurse_index(page_number, node_proc, leaf_proc, link_proc)
    _recurse_index(page(page_number), node_proc, leaf_proc, link_proc)
  end
end
