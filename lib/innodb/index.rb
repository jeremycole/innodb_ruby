# -*- encoding : utf-8 -*-
# An InnoDB index B-tree, given an Innodb::Space and a root page number.
class Innodb::Index
  attr_reader :root
  attr_reader :stats
  attr_accessor :debug
  attr_accessor :record_describer

  def initialize(space, root_page_number, record_describer=nil)
    @debug = false
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

    reset_stats
  end

  def page(page_number)
    page = @space.page(page_number)
    page.record_describer = @record_describer
    page
  end

  def reset_stats
    @stats = Hash.new(0)
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
      page = page(record[:child_page_number])
      record = page.first_record
    end
    page if page.level == level
  end

  # Return the file segment with the given name from the fseg header.
  def fseg(name)
    @root.fseg_header[name]
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
      page = page(page.next)
    end
  end

  # Iterate through all pages at the given level by finding the first page
  # and following the next pointers in each page.
  def each_page_at_level(level)
    unless block_given?
      return enum_for(:each_page_at_level, level)
    end

    each_page_from(first_page_at_level(level)) { |page| yield page }
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

  # Compare two arrays of fields to determine if they are equal. This follows
  # the same comparison rules as strcmp and others:
  #   0 = a is equal to b
  #   -1 = a is less than b
  #   +1 = a is greater than b
  def compare_key(a, b)
    @stats[:compare_key] += 1

    return 0 if a.nil? && b.nil?
    return -1 if a.nil? || (!b.nil? && a.size < b.size)
    return +1 if b.nil? || (!a.nil? && a.size > b.size)

    a.each_index do |i|
      @stats[:compare_key_field_comparison] += 1
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
  def linear_search_from_cursor(page, cursor, key)
    @stats[:linear_search_from_cursor] += 1

    this_rec = cursor.record

    if @debug
      puts "linear_search_from_cursor: page=%i, level=%i, start=(%s)" % [
        page.offset,
        page.level,
        this_rec && this_rec[:key].join(", "),
      ]
    end

    # Iterate through all records until finding either a matching record or
    # one whose key is greater than the desired key.
    while this_rec && next_rec = cursor.record
      @stats[:linear_search_from_cursor_record_scans] += 1

      if @debug
        puts "linear_search_from_cursor: page=%i, level=%i, current=(%s)" % [
          page.offset,
          page.level,
          this_rec && this_rec[:key].join(", "),
        ]
      end

      # If we reach supremum, return the last non-system record we got.
      return this_rec if next_rec[:header][:type] == :supremum

      if compare_key(key, this_rec[:key]) < 0
        return this_rec
      end

      if (compare_key(key, this_rec[:key]) >= 0) &&
        (compare_key(key, next_rec[:key]) < 0)
        # The desired key is either an exact match for this_rec or is greater
        # than it but less than next_rec. If this is a non-leaf page, that
        # will mean that the record will fall on the leaf page this node
        # pointer record points to, if it exists at all.
        return this_rec
      end

      this_rec = next_rec
    end

    this_rec
  end

  # Search or a record within a single page using the page directory to limit
  # the number of record comparisons required. Once the last page directory
  # entry closest to but not greater than the key is found, fall back to
  # linear search using linear_search_from_cursor to find the closest record
  # whose key is not greater than the desired key. (If an exact match is
  # desired, the returned record must be checked in the same way as the above
  # linear_search_from_cursor function.)
  def binary_search_by_directory(page, dir, key)
    @stats[:binary_search_by_directory] += 1

    return nil if dir.empty?

    # Split the directory at the mid-point (using integer math, so the division
    # is rounding down). Retrieve the record that sits at the mid-point.
    mid = ((dir.size-1) / 2)
    rec = page.record(dir[mid])

    if @debug
      puts "binary_search_by_directory: page=%i, level=%i, dir.size=%i, dir[%i]=(%s)" % [
        page.offset,
        page.level,
        dir.size,
        mid,
        rec[:key] && rec[:key].join(", "),
      ]
    end

    # The mid-point record was the infimum record, which is not comparable with
    # compare_key, so we need to just linear scan from here. If the mid-point
    # is the beginning of the page there can't be many records left to check
    # anyway.
    if rec[:header][:type] == :infimum
      return linear_search_from_cursor(page, page.record_cursor(rec[:next]), key)
    end

    # Compare the desired key to the mid-point record's key.
    case compare_key(key, rec[:key])
    when 0
      # An exact match for the key was found. Return the record.
      @stats[:binary_search_by_directory_exact_match] += 1
      rec
    when +1
      # The mid-point record's key is less than the desired key.
      if dir.size > 2
        # There are more entries remaining from the directory, recurse again
        # using binary search on the right half of the directory, which
        # represents values greater than or equal to the mid-point record's
        # key.
        @stats[:binary_search_by_directory_recurse_right] += 1
        binary_search_by_directory(page, dir[mid...dir.size], key)
      else
        next_rec = page.record(dir[mid+1])
        next_key = next_rec && compare_key(key, next_rec[:key])
        if dir.size == 1 || next_key == -1 || next_key == 0
          # This is the last entry remaining from the directory, or our key is
          # greater than rec and less than rec+1's key. Use linear search to
          # find the record starting at rec.
          @stats[:binary_search_by_directory_linear_search] += 1
          linear_search_from_cursor(page, page.record_cursor(rec[:offset]), key)
        elsif next_key == +1
          @stats[:binary_search_by_directory_linear_search] += 1
          linear_search_from_cursor(page, page.record_cursor(next_rec[:offset]), key)
        else
          nil
        end
      end
    when -1
      # The mid-point record's key is greater than the desired key.
      if dir.size == 1
        # If this is the last entry remaining from the directory, we didn't
        # find anything workable.
        @stats[:binary_search_by_directory_empty_result] += 1
        nil
      else
        # Recurse on the left half of the directory, which represents values
        # less than the mid-point record's key.
        @stats[:binary_search_by_directory_recurse_left] += 1
        binary_search_by_directory(page, dir[0...mid], key)
      end
    end
  end

  # Search for a record within the entire index, walking down the non-leaf
  # pages until a leaf page is found, and then verifying that the record
  # returned on the leaf page is an exact match for the key. If a matching
  # record is not found, nil is returned (either because linear_search_in_page
  # returns nil breaking the loop, or because compare_key returns non-zero).
  def linear_search(key)
    @stats[:linear_search] += 1

    page = @root

    if @debug
      puts "linear_search: root=%i, level=%i, key=(%s)" % [
        page.offset,
        page.level,
        key.join(", "),
      ]
    end

    while rec =
      linear_search_from_cursor(page, page.record_cursor(page.infimum[:next]), key)
      if page.level > 0
        # If we haven't reached a leaf page yet, move down the tree and search
        # again using linear search.
        page = page(rec[:child_page_number])
      else
        # We're on a leaf page, so return the page and record if there is a
        # match. If there is no match, break the loop and cause nil to be
        # returned.
        return page, rec if compare_key(key, rec[:key]) == 0
        break
      end
    end
  end

  # Search for a record within the entire index like linear_search, but use
  # the page directory to search while making as few record comparisons as
  # possible. If a matching record is not found, nil is returned.
  def binary_search(key)
    @stats[:binary_search] += 1

    page = @root

    if @debug
      puts "binary_search: root=%i, level=%i, key=(%s)" % [
        page.offset,
        page.level,
        key.join(", "),
      ]
    end

    # Remove supremum from the page directory, since nothing can be scanned
    # linearly from there anyway.
    while rec = binary_search_by_directory(page, page.directory[0...-1], key)
      if page.level > 0
        # If we haven't reached a leaf page yet, move down the tree and search
        # again using binary search.
        page = page(rec[:child_page_number])
      else
        # We're on a leaf page, so return the page and record if there is a
        # match. If there is no match, break the loop and cause nil to be
        # returned.
        return page, rec if compare_key(key, rec[:key]) == 0
        break
      end
    end
  end

end
