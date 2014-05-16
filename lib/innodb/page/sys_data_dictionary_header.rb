# -*- encoding : utf-8 -*-

class Innodb::Page::SysDataDictionaryHeader < Innodb::Page
  # The position of the data dictionary header within the page.
  def pos_data_dictionary_header
    pos_page_body
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

  def each_region
    unless block_given?
      return enum_for(:each_region)
    end

    super do |region|
      yield region
    end

    yield({
      :offset => pos_data_dictionary_header,
      :length => size_data_dictionary_header,
      :name => :data_dictionary_header,
      :info => "Data Dictionary Header",
    })

    nil
  end

  def dump
    super

    puts
    puts "data_dictionary header:"
    pp data_dictionary_header
  end
end
