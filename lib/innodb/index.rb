# -*- encoding : utf-8 -*-

# An InnoDB index B-tree, given an Innodb::Space and a root page number.
class Innodb::Index
  attr_reader :root
  attr_reader :space
  attr_accessor :record_describer

  def initialize(space, root_page_number, record_describer=nil)
    @space = space
    @record_describer = record_describer || space.record_describer

    @root = page(root_page_number)

    unless @root
      raise "Page #{root_page_number} couldn't be read"
    end

    # The root page should be an index page.
    unless @root.type == :INDEX
      raise "Page #{root_page_number} is a #{@root.type} page, not an INDEX page"
    end

    # The root page should be the only page at its level.
    unless @root.prev.nil? && @root.next.nil?
      raise "Page #{root_page_number} is a node page, but not appear to be the root; it has previous page and next page pointers"
    end
  end

  def page(page_number)
    page = @space.page(page_number) or
      raise "Page #{page_number} couldn't be read"
    page.record_describer = @record_describer
    page
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
      child_page = page(child_page_number)
      child_page.record_describer = record_describer
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
  def min_page_at_level(level)
    page = @root
    record = @root.min_record
    while record && page.level > level
      page = page(record.child_page_number)
      record = page.min_record
    end
    page if page.level == level
  end

  # Return the minimum record in the index.
  def min_record
    if min_page = min_page_at_level(0)
      min_page.min_record
    end
  end

  # Return the last leaf page in the index by walking down the right side
  # of the B-tree until a page at the given level is encountered.
  def max_page_at_level(level)
    page = @root
    record = @root.max_record
    while record && page.level > level
      page = page(record.child_page_number)
      record = page.max_record
    end
    page if page.level == level
  end

  # Return the maximum record in the index.
  def max_record
    if max_page = max_page_at_level(0)
      max_page.max_record
    end
  end

  # Return the file segment with the given name from the fseg header.
  def fseg(name)
    @root.fseg_header[name]
  end

  def field_names
    record_describer.field_names
  end

  # Iterate through all file segments in the index.
  def each_fseg
    unless block_given?
      return enum_for(:each_fseg)
    end

    [:internal, :leaf].each do |fseg_name|
      yield fseg_name, fseg(fseg_name)
    end
  end

  # Iterate through all lists in a given fseg.
  def each_fseg_list(fseg)
    unless block_given?
      return enum_for(:each_fseg_list, fseg)
    end

    fseg.each_list do |list_name, list|
      yield list_name, list
    end
  end

  # Iterate through all frag pages in a given fseg.
  def each_fseg_frag_page(fseg)
    unless block_given?
      return enum_for(:each_fseg_frag_page, fseg)
    end

    fseg.frag_array_pages.each do |page_number|
      yield page_number, page(page_number)
    end
  end

  # Iterate through all pages at this level starting with the provided page.
  def each_page_from(page)
    unless block_given?
      return enum_for(:each_page_from, page)
    end

    while page && page.type == :INDEX
      yield page
      break unless page.next
      page = page(page.next)
    end
  end

  # Iterate through all pages at the given level by finding the first page
  # and following the next pointers in each page.
  def each_page_at_level(level)
    unless block_given?
      return enum_for(:each_page_at_level, level)
    end

    each_page_from(min_page_at_level(level)) { |page| yield page }
  end

  # Iterate through all records on all leaf pages in ascending order.
  def each_record
    unless block_given?
      return enum_for(:each_record)
    end

    each_page_at_level(0) do |page|
      page.each_record do |record|
        yield record
      end
    end
  end

  # Search for a record within the entire index, walking down the non-leaf
  # pages until a leaf page is found, and then verifying that the record
  # returned on the leaf page is an exact match for the key. If a matching
  # record is not found, nil is returned (either because linear_search_in_page
  # returns nil breaking the loop, or because compare_key returns non-zero).
  def linear_search(key)
    Innodb::Stats.increment :linear_search

    page = @root

    if Innodb.debug?
      puts "linear_search: root=%i, level=%i, key=(%s)" % [
        page.offset,
        page.level,
        key.join(", "),
      ]
    end

    while rec =
      page.linear_search_from_cursor(page.record_cursor(page.infimum.next), key)
      if page.level > 0
        # If we haven't reached a leaf page yet, move down the tree and search
        # again using linear search.
        page = page(rec.child_page_number)
      else
        # We're on a leaf page, so return the page and record if there is a
        # match. If there is no match, break the loop and cause nil to be
        # returned.
        return rec if rec.compare_key(key) == 0
        break
      end
    end
  end

  # Search for a record within the entire index like linear_search, but use
  # the page directory to search while making as few record comparisons as
  # possible. If a matching record is not found, nil is returned.
  def binary_search(key)
    Innodb::Stats.increment :binary_search

    page = @root

    if Innodb.debug?
      puts "binary_search: root=%i, level=%i, key=(%s)" % [
        page.offset,
        page.level,
        key.join(", "),
      ]
    end

    # Remove supremum from the page directory, since nothing can be scanned
    # linearly from there anyway.
    while rec = page.binary_search_by_directory(page.directory[0...-1], key)
      if page.level > 0
        # If we haven't reached a leaf page yet, move down the tree and search
        # again using binary search.
        page = page(rec.child_page_number)
      else
        # We're on a leaf page, so return the page and record if there is a
        # match. If there is no match, break the loop and cause nil to be
        # returned.
        return rec if rec.compare_key(key) == 0
        break
      end
    end
  end

  # A cursor to walk the index (cursor) forwards or backward starting with
  # a given record, or the minimum (:min) or maximum (:max) record in the
  # index.
  class IndexCursor
    def initialize(index, record, direction)
      Innodb::Stats.increment :index_cursor_create
      @index = index
      @direction = direction
      case record
      when :min
        # Start at the minimum record on the minimum page in the index.
        @page = index.min_page_at_level(0)
        @page_cursor = @page.record_cursor(:min, direction)
      when :max
        # Start at the maximum record on the maximum page in the index.
        @page = index.max_page_at_level(0)
        @page_cursor = @page.record_cursor(:max, direction)
      else
        # Start at the record provided.
        @page = record.page
        @page_cursor = @page.record_cursor(record.offset, direction)
      end
    end

    # Return the next record in the order defined when the cursor was created.
    def record
      if rec = @page_cursor.record
        return rec
      end

      case @direction
      when :forward
        next_record
      when :backward
        prev_record
      end
    end

    # Iterate through all records in the cursor.
    def each_record
      unless block_given?
        return enum_for(:each_record)
      end

      while rec = record
        yield rec
      end
    end

    private

    # Move the cursor to a new starting position in a given page.
    def move_cursor(page, record)
      unless @page = @index.page(page)
        raise "Failed to load page"
      end

      unless @page_cursor = @page.record_cursor(record, @direction)
        raise "Failed to position cursor"
      end
    end

    # Move to the next record in the forward direction and return it.
    def next_record
      Innodb::Stats.increment :index_cursor_next_record

      if rec = @page_cursor.record
        return rec
      end

      unless next_page = @page.next
        return nil
      end

      move_cursor(next_page, :min)

      @page_cursor.record
    end

    # Move to the previous record in the backward direction and return it.
    def prev_record
      Innodb::Stats.increment :index_cursor_prev_record

      if rec = @page_cursor.record
        return rec
      end

      unless prev_page = @page.prev
        return nil
      end

      move_cursor(prev_page, :max)

      @page_cursor.record
    end
  end

  # Return an IndexCursor starting at the given record (an Innodb::Record,
  # :min, or :max) and cursor in the direction given (:forward or :backward).
  def cursor(record=:min, direction=:forward)
    IndexCursor.new(self, record, direction)
  end
end
