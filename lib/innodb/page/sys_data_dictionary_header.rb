class Innodb::Page::SysDataDictionaryHeader < Innodb::Page
  # The position of the data dictionary header within the page.
  def pos_data_dictionary_header
    pos_fil_header + size_fil_header
  end

  # The size of the data dictionary header.
  def size_data_dictionary_header
    ((8 * 3) + (4 * 7) + 4 + Innodb::FsegEntry::SIZE)
  end

  # Parse the data dictionary header from the page.
  def data_dictionary_header
    cursor(pos_data_dictionary_header).name("data_dictionary_header") do |c|
      {
        :max_row_id => c.name("max_row_id") { c.get_uint64 },
        :max_table_id => c.name("max_table_id") { c.get_uint64 },
        :max_index_id => c.name("max_index_id") { c.get_uint64 },
        :max_space_id => c.name("max_space_id") { c.get_uint32 },
        :unused_mix_id_low => c.name("unused_mix_id_low") { c.get_uint32 },
        :indexes => c.name("indexes") {{
          :SYS_TABLES => c.name("SYS_TABLES") {{
            :PRIMARY => c.name("PRIMARY") { c.get_uint32 },
            :ID      => c.name("ID")      { c.get_uint32 }, 
          }},
          :SYS_COLUMNS => c.name("SYS_COLUMNS") {{
            :PRIMARY => c.name("PRIMARY") { c.get_uint32 },
          }},
          :SYS_INDEXES => c.name("SYS_INDEXES") {{
            :PRIMARY => c.name("PRIMARY") { c.get_uint32 },
          }},
          :SYS_FIELDS => c.name("SYS_FIELDS") {{
            :PRIMARY => c.name("PRIMARY") { c.get_uint32 },
          }}
        }},
        :unused_space => c.name("unused_space") { c.get_bytes(4) },
        :fseg => c.name("fseg") { Innodb::FsegEntry.get_inode(@space, c) },
      }
    end
  end

  # Iterate through all indexes in the data dictionary, yielding the table
  # name, index name, and the index itself as an Innodb::Index.
  def each_index
    unless block_given?
      return enum_for(:each_index)
    end

    data_dictionary_header[:indexes].each do |table_name, indexes|
      indexes.each do |index_name, root_page_number|
        yield table_name, index_name, @space.index(root_page_number)
      end
    end
  end

  def dump
    super

    puts
    puts "data_dictionary header:"
    pp data_dictionary_header
  end

  # A record describer for SYS_TABLES clustered records.
  class SYS_TABLES_PRIMARY
    def self.cursor_sendable_description(page)
      {
        :type => :clustered,
        :key => [
          ["VARCHAR(100)",      :NOT_NULL],       # NAME
        ],
        :row => [
          [:BIGINT, :UNSIGNED,  :NOT_NULL],       # ID
          [:INT,    :UNSIGNED,  :NOT_NULL],       # N_COLS
          [:INT,    :UNSIGNED,  :NOT_NULL],       # TYPE
          [:BIGINT, :UNSIGNED,  :NOT_NULL],       # MIX_ID
          [:INT,    :UNSIGNED,  :NOT_NULL],       # MIX_LEN
          ["VARCHAR(100)"],                       # CLUSTER_NAME
          [:INT,    :UNSIGNED,  :NOT_NULL],       # SPACE
        ]
      }
    end
  end

  RECORD_DESCRIBERS = {
    :SYS_TABLES  => { :PRIMARY => SYS_TABLES_PRIMARY, :ID => nil },
    :SYS_COLUMNS => { :PRIMARY => nil },
    :SYS_INDEXES => { :PRIMARY => nil },
    :SYS_FIELDS  => { :PRIMARY => nil },
  }
end
