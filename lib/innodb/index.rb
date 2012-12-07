# An InnoDB index B-tree, given an Innodb::Space and a root page number.
class Innodb::Index
  attr_reader :root

  def initialize(space, root_page_number)
    @space = space
    @root = @space.page(root_page_number)

    unless @root
      raise "Page #{root_page_number} couldn't be read"
    end

    # The root page should be an index page.
    unless @root.type == :INDEX
      raise "Page #{root_page_number} is a #{@root.type} page, not an INDEX page"
    end

    # The root page should not be a leaf page.
    unless @root.level > 0
      raise "Page #{root_page_number} is a leaf page"
    end

    # The root page should be the only page at its level.
    unless @root.prev.nil? && @root.next.nil?
      raise "Page #{root_page_number} is a node page, but not appear to be the root; it has previous page and next page pointers"
    end
  end

  # A helper function to access the index ID in the page header.
  def id
    @root.page_header[:index_id]
  end

  # Internal method used by recurse.
  def _recurse(parent_page, page_proc, link_proc, depth=0)
    if page_proc && parent_page.type == :INDEX
      page_proc.call(parent_page, depth)
    end

    parent_page.each_child_page do |child_page_number, child_min_key|
      child_page = @space.page(child_page_number)
      child_page.record_describer = @space.record_describer
      if child_page.type == :INDEX
        if link_proc
          link_proc.call(parent_page, child_page, child_min_key, depth+1)
        end
        _recurse(child_page, page_proc, link_proc, depth+1)
      end
    end
  end

  # Walk an index tree depth-first, calling procs for each page and link
  # in the tree.
  def recurse(page_proc, link_proc)
    _recurse(@root, page_proc, link_proc)
  end

  # Return the first leaf page in the index by walking down the left side
  # of the B-tree until a page at the given level is encountered.
  def first_page_at_level(level)
    page = @root
    record = @root.first_record
    while record && page.level > level
      page = @space.page(record[:child_page_number])
      record = page.first_record
    end
    page if page.level == level
  end

  # Iterate through all pages at this level starting with the provided page.
  def each_page_from(page)
    while page && page.type == :INDEX
      yield page
      page = @space.page(page.next)
    end
  end

  # Iterate through all pages at the given level by finding the first page
  # and following the next pointers in each page.
  def each_page_at_level(level)
    each_page_from(first_page_at_level(level)) { |page| yield page }
  end

  # Iterate through all records on all leaf pages in ascending order.
  def each_record
    each_page_at_level(0) do |page|
      page.each_record do |record|
        yield record
      end
    end
  end
end