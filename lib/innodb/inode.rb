# frozen_string_literal: true

require "forwardable"

module Innodb
  class Inode
    extend Forwardable

    Header = Struct.new(
      :offset,
      :fseg_id,
      :not_full_n_used,
      :free,
      :not_full,
      :full,
      :magic_n,
      :frag_array,
      keyword_init: true
    )

    # The number of "slots" (each representing one page) in the fragment array
    # within each Inode entry.
    FRAG_ARRAY_N_SLOTS = 32 # FSP_EXTENT_SIZE / 2

    # The size (in bytes) of each slot in the fragment array.
    FRAG_SLOT_SIZE = 4

    # A magic number which helps determine if an Inode structure is in use
    # and populated with valid data.
    MAGIC_N_VALUE	= 97_937_874

    # The size (in bytes) of an Inode entry.
    SIZE = (16 + (3 * Innodb::List::BASE_NODE_SIZE) +
      (FRAG_ARRAY_N_SLOTS * FRAG_SLOT_SIZE))

    LISTS = %i[
      free
      not_full
      full
    ].freeze

    # Read an array of page numbers (32-bit integers, which may be nil) from
    # the provided cursor.
    def self.page_number_array(size, cursor)
      size.times.map do |n|
        cursor.name("page[#{n}]") do |c|
          Innodb::Page.maybe_undefined(c.read_uint32)
        end
      end
    end

    # Construct a new Inode by reading an FSEG header from a cursor.
    def self.new_from_cursor(space, cursor)
      Innodb::Inode.new(
        space,
        Header.new(
          offset: cursor.position,
          fseg_id: cursor.name("fseg_id") { cursor.read_uint64 },
          not_full_n_used: cursor.name("not_full_n_used") { cursor.read_uint32 },
          free: cursor.name("list[free]") { Innodb::List::Xdes.new(space, Innodb::List.get_base_node(cursor)) },
          not_full: cursor.name("list[not_full]") { Innodb::List::Xdes.new(space, Innodb::List.get_base_node(cursor)) },
          full: cursor.name("list[full]") { Innodb::List::Xdes.new(space, Innodb::List.get_base_node(cursor)) },
          magic_n: cursor.name("magic_n") { cursor.read_uint32 },
          frag_array: cursor.name("frag_array") { page_number_array(FRAG_ARRAY_N_SLOTS, cursor) }
        )
      )
    end

    attr_accessor :space
    attr_accessor :header

    def initialize(space, header)
      @space = space
      @header = header
    end

    def_delegator :header, :offset
    def_delegator :header, :fseg_id
    def_delegator :header, :not_full_n_used
    def_delegator :header, :free
    def_delegator :header, :not_full
    def_delegator :header, :full
    def_delegator :header, :magic_n
    def_delegator :header, :frag_array

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
      frag_array.reject(&:nil?)
    end

    # Helper method to count non-nil fragment pages.
    def frag_array_n_used
      frag_array_pages.count
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
      total_pages.positive? ? 100.0 * (used_pages.to_f / total_pages) : 0.0
    end

    # Return a list from the fseg, given its name as a symbol.
    def list(name)
      return unless LISTS.include?(name)

      header[name]
    end

    # Iterate through all lists, yielding the list name and the list itself.
    def each_list
      return enum_for(:each_list) unless block_given?

      LISTS.each do |name|
        yield name, list(name)
      end

      nil
    end

    # Iterate through the fragment array followed by all lists, yielding the
    # page number. This allows a convenient way to identify all pages that are
    # part of this inode.
    def each_page_number(&block)
      return enum_for(:each_page_number) unless block_given?

      frag_array_pages.each(&block)

      each_list do |_fseg_name, fseg_list|
        fseg_list.each do |xdes|
          xdes.each_page_status(&block)
        end
      end

      nil
    end

    # Iterate through the page as associated with this inode using the
    # each_page_number method, and yield the page number and page.
    def each_page
      return enum_for(:each_page) unless block_given?

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
      pp header
    end
  end
end
