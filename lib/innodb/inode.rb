# -*- encoding : utf-8 -*-

class Innodb::Inode
  # The number of "slots" (each representing one page) in the fragment array
  # within each Inode entry.
  FRAG_ARRAY_N_SLOTS  = 32 # FSP_EXTENT_SIZE / 2

  # The size (in bytes) of each slot in the fragment array.
  FRAG_SLOT_SIZE      = 4

  # A magic number which helps determine if an Inode structure is in use
  # and populated with valid data.
  MAGIC_N_VALUE	= 97937874

  # The size (in bytes) of an Inode entry.
  SIZE = (16 + (3 * Innodb::List::BASE_NODE_SIZE) +
    (FRAG_ARRAY_N_SLOTS * FRAG_SLOT_SIZE))

  # Read an array of page numbers (32-bit integers, which may be nil) from
  # the provided cursor.
  def self.page_number_array(size, cursor)
    size.times.map do |n|
      cursor.name("page[#{n}]") do |c|
        Innodb::Page.maybe_undefined(c.get_uint32)
      end
    end
  end

  # Construct a new Inode by reading an FSEG header from a cursor.
  def self.new_from_cursor(space, cursor)
    data = {
      :offset => cursor.position,
      :fseg_id => cursor.name("fseg_id") {
        cursor.get_uint64
      },
      :not_full_n_used => cursor.name("not_full_n_used") {
        cursor.get_uint32
      },
      :free => cursor.name("list[free]") { 
        Innodb::List::Xdes.new(space, Innodb::List.get_base_node(cursor))
      },
      :not_full => cursor.name("list[not_full]") { 
        Innodb::List::Xdes.new(space, Innodb::List.get_base_node(cursor))
      },
      :full => cursor.name("list[full]") { 
        Innodb::List::Xdes.new(space, Innodb::List.get_base_node(cursor))
      },
      :magic_n => cursor.name("magic_n") {
        cursor.get_uint32
      },
      :frag_array => cursor.name("frag_array") { 
        page_number_array(FRAG_ARRAY_N_SLOTS, cursor)
      },
    }

    Innodb::Inode.new(space, data)
  end

  attr_accessor :space

  def initialize(space, data)
    @space = space
    @data = data
  end

  def offset;           @data[:offset];           end
  def fseg_id;          @data[:fseg_id];          end
  def not_full_n_used;  @data[:not_full_n_used];  end
  def free;             @data[:free];             end
  def not_full;         @data[:not_full];         end
  def full;             @data[:full];             end
  def magic_n;          @data[:magic_n];          end
  def frag_array;       @data[:frag_array];       end

  def inspect
    "<%s space=%s, fseg=%i>" % [
      self.class.name,
      space.inspect,
      fseg_id,
    ]
  end

  # Helper method to determine if an Inode is in use. Inodes that are not in
  # use have an fseg_id of 0.
  def allocated?
    fseg_id != 0
  end

  # Helper method to return an array of only non-nil fragment pages.
  def frag_array_pages
    frag_array.select { |n| ! n.nil? }
  end

  # Helper method to count non-nil fragment pages.
  def frag_array_n_used
    frag_array.inject(0) { |n, i| n += 1 if i; n }
  end

  # Calculate the total number of pages in use (not free) within this fseg.
  def used_pages
    frag_array_n_used + not_full_n_used +
      (full.length * @space.pages_per_extent)
  end

  # Calculate the total number of pages within this fseg.
  def total_pages
    frag_array_n_used +
      (free.length * @space.pages_per_extent) +
      (not_full.length * @space.pages_per_extent) +
      (full.length * @space.pages_per_extent)
  end

  # Calculate the fill factor of this fseg, in percent.
  def fill_factor
    total_pages > 0 ? 100.0 * (used_pages.to_f / total_pages.to_f) : 0.0
  end

  # Return an array of lists within an fseg.
  def lists
    [:free, :not_full, :full]
  end

  # Return a list from the fseg, given its name as a symbol.
  def list(name)
    @data[name] if lists.include? name
  end

  # Iterate through all lists, yielding the list name and the list itself.
  def each_list
    unless block_given?
      return enum_for(:each_list)
    end

    lists.each do |name|
      yield name, list(name)
    end

    nil
  end

  # Iterate through the fragment array followed by all lists, yielding the
  # page number. This allows a convenient way to identify all pages that are
  # part of this inode.
  def each_page_number
    unless block_given?
      return enum_for(:each_page_number)
    end

    frag_array_pages.each do |page_number|
      yield page_number
    end

    each_list do |fseg_name, fseg_list|
      fseg_list.each do |xdes|
        xdes.each_page_status do |page_number|
          yield page_number
        end
      end
    end

    nil
  end

  # Iterate through the page as associated with this inode using the
  # each_page_number method, and yield the page number and page.
  def each_page
    unless block_given?
      return enum_for(:each_page)
    end

    each_page_number do |page_number|
      yield page_number, space.page(page_number)
    end

    nil
  end

  # Compare one Innodb::Inode to another.
  def ==(other)
    fseg_id == other.fseg_id if other
  end

  # Dump a summary of this object for debugging purposes.
  def dump
    pp @data
  end
end
