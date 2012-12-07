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

  # Return the type of node that the given page represents in the index tree.
  def node_type(page)
    if @root.offset == page.offset
      :root
    elsif page.level == 0
      :leaf
    else
      :internal
    end
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

  # Compare two arrays of fields to determine if they are equal. This follows
  # the same comparison rules as strcmp and others:
  #   0 = a is equal to b
  #   -1 = a is less than b
  #   +1 = a is greater than b
  def compare_key(a, b)
    return -1 if a.size < b.size
    return +1 if a.size > b.size
    a.each_index do |i|
      return -1 if a[i] < b[i]
      return +1 if a[i] > b[i]
    end
    return 0
  end

  # Search for a record within a single page, and return either a perfect
  # match for the key, or the last record closest to they key but not greater
  # than the key. (If an exact match is desired, compare_key must be used to
  # check if the returned record matches. This makes the function useful for
  # search in both leaf and non-leaf pages.)
  def linear_search_in_page(page, key)
    c = page.record_cursor(page.infimum[:next])
    this_rec = c.record
    while next_rec = c.record
      return this_rec if next_rec == page.supremum
      if (compare_key(key, this_rec[:key]) >= 0) &&
        (compare_key(key, next_rec[:key]) < 0)
        return this_rec
      end
      this_rec = next_rec
    end
    this_rec
  end

  # Search for a record within the entire index, walking down the non-leaf
  # pages until a leaf page is found, and then verifying that the record
  # returned on the leaf page is an exact match for the key. If a matching
  # record is not found, nil is returned (either because linear_search_in_page
  # returns nil breaking the loop, or because compare_key returns non-zero).
  def linear_search(key)
    page = @root

    while rec = linear_search_in_page(page, key)
      if page.level > 0
        page = @space.page(rec[:child_page_number])
      else
        return page, rec if compare_key(key, rec[:key]) == 0
        break
      end
    end
  end
end
