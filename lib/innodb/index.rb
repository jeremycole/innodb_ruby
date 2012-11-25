# An InnoDB index B-tree, given an Innodb::Space and a root page number.
class Innodb::Index
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

  # Internal method used by recurse.
  def _recurse(parent_page, page_proc, link_proc, depth=0)
    if page_proc && parent_page.type == :INDEX
      page_proc.call(parent_page, depth)
    end

    parent_page.each_child_page do |child_page_number, child_min_key|
      child_page = @space.page(child_page_number)
      child_page.record_formatter = @space.record_formatter
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
  # of the B-tree until a leaf page is encountered.
  def first_leaf_page
    page = @root
    record = @root.first_record
    while record && page.level > 0
      page = @space.page(record[:child_page_number])
      record = page.first_record
    end
    page
  end

  # Iterate through all leaf pages by finding the first leaf page and following
  # the next pointers in each page.
  def each_leaf_page
    page = first_leaf_page
    while page.type == :INDEX
      yield page
      page = @space.page(page.next)
    end
  end
end