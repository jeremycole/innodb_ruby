# -*- encoding : utf-8 -*-

require "innodb/data_dictionary"

class Innodb::Page::SysDataDictionaryHeader < Innodb::Page
  RECORD_DESCRIBERS = {
    :SYS_TABLES  => {
      :PRIMARY => Innodb::DataDictionary::SYS_TABLES_PRIMARY,
      :ID => Innodb::DataDictionary::SYS_TABLES_ID
    },
    :SYS_COLUMNS => { :PRIMARY => Innodb::DataDictionary::SYS_COLUMNS_PRIMARY },
    :SYS_INDEXES => { :PRIMARY => Innodb::DataDictionary::SYS_INDEXES_PRIMARY },
    :SYS_FIELDS  => { :PRIMARY => Innodb::DataDictionary::SYS_FIELDS_PRIMARY },
  }

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

  def index(table_name, index_name)
    unless table_entry = data_dictionary_header[:indexes][table_name]
      raise "Unknown data dictionary table #{table_name}"
    end

    unless index_root_page = table_entry[index_name]
      raise "Unknown data dictionary index #{table_name}.#{index_name}"
    end

    # If we have a record describer for this index, load it.
    record_describer = RECORD_DESCRIBERS[table_name] &&
                       RECORD_DESCRIBERS[table_name][index_name]

    @space.index(index_root_page, record_describer.new)
  end

  # Iterate through all indexes in the data dictionary, yielding the table
  # name, index name, and the index itself as an Innodb::Index.
  def each_index
    unless block_given?
      return enum_for(:each_index)
    end

    data_dictionary_header[:indexes].each do |table_name, indexes|
      indexes.each do |index_name, root_page_number|
        yield table_name, index_name, index(table_name, index_name)
      end
    end
  end

  def dump
    super

    puts
    puts "data_dictionary header:"
    pp data_dictionary_header
  end
end
