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

  def self.new_from_cursor(space, cursor)
    data = {
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

  def initialize(space, data)
    @space = space
    @data = data
  end

  def fseg_id;          @data[:fseg_id];          end
  def not_full_n_used;  @data[:not_full_n_used];  end
  def free;             @data[:free];             end
  def not_full;         @data[:not_full];         end
  def full;             @data[:full];             end
  def magic_n;          @data[:magic_n];          end
  def frag_array;       @data[:frag_array];       end

  def dump
    pp @data
  end
end