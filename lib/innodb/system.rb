# frozen_string_literal: true

# A class representing an entire InnoDB system, having a system tablespace
# and any number of attached single-table tablespaces.
module Innodb
  class System
    # A hash of configuration options by configuration key.
    attr_reader :config

    # A hash of spaces by space ID.
    attr_reader :spaces

    # Array of space names for which a space file was not found.
    attr_reader :orphans

    # The Innodb::DataDictionary for this system.
    attr_reader :data_dictionary

    # The space ID of the system space, always 0.
    SYSTEM_SPACE_ID = 0

    # The space ID of the mysql.ibd space, always 4294967294 (2**32-2).
    MYSQL_SPACE_ID = 4_294_967_294

    def initialize(arg, data_directory: nil)
      @data_dictionary = Innodb::DataDictionary.new

      if arg.is_a?(Array) && arg.size > 1
        data_filenames = arg
      else
        arg = arg.first if arg.is_a?(Array)
        if File.directory?(arg)
          data_filenames = Dir.glob("#{arg}/ibdata?").sort
          raise "Couldn't find any ibdata files in #{arg}" if data_filenames.empty?
        else
          data_filenames = [arg]
        end
      end

      @spaces = {}
      @orphans = []
      @config = {
        data_directory: data_directory || File.dirname(data_filenames.first),
      }

      add_space_file(data_filenames)

      add_mysql_space_file
      add_all_ibd_files

      @internal_data_dictionary = if system_space.page(0).prev > 80_000 # ugh
                                    Innodb::SdiDataDictionary.new(self)
                                  else
                                    Innodb::SysDataDictionary.new(self)
                                  end
      @internal_data_dictionary.populate_data_dictionary
      data_dictionary.refresh

      data_dictionary.tables.each do |table|
        add_table(table.name) unless spaces[table.tablespace.innodb_space_id]
      end
    end

    def data_directory
      config[:data_directory]
    end

    # A helper to get the system space.
    def system_space
      spaces[SYSTEM_SPACE_ID]
    end

    def mysql_space
      spaces[MYSQL_SPACE_ID]
    end

    # Add an already-constructed Innodb::Space object.
    def add_space(space)
      raise "Object was not an Innodb::Space" unless space.is_a?(Innodb::Space)

      spaces[space.space_id] = space
    end

    # Add a space by filename.
    def add_space_file(space_filenames)
      space = Innodb::Space.new(space_filenames, innodb_system: self)
      add_space(space) unless spaces[space.space_id]
    end

    # Add an orphaned space.
    def add_space_orphan(space_file)
      orphans << space_file
    end

    # Add a space by table name, constructing an appropriate filename
    # from the provided table name.
    def add_table(table_name)
      space_file = File.join(config[:data_directory], format("%s.ibd", table_name))

      if File.exist?(space_file)
        add_space_file(space_file)
      else
        add_space_orphan(table_name)
      end
    end

    # Return an Innodb::Space object for a given space ID, looking up
    # and adding the single-table space if necessary.
    def space(space_id)
      return spaces[space_id] if spaces[space_id]

      unless (table = data_dictionary.tables.find(innodb_space_id: space_id))
        raise "Table with space ID #{space_id} not found"
      end

      add_table(table.name)

      spaces[space_id]
    end

    def space_by_table_name(table_name)
      space_id = data_dictionary.tables.find(name: table_name)&.tablespace&.innodb_space_id

      spaces[space_id] if space_id
    end

    def add_mysql_space_file
      mysql_ibd = File.join(data_directory, "mysql.ibd")
      add_space_file(mysql_ibd) if File.exist?(mysql_ibd)
    end

    # Iterate through all table names.
    def each_ibd_file_name(&block)
      return enum_for(:each_ibd_file_name) unless block_given?

      Dir.glob(File.join(data_directory, "**/*.ibd"))
         .map { |f| f.sub(File.join(data_directory, "/"), "") }.each(&block)

      nil
    end

    def add_all_ibd_files
      each_ibd_file_name do |file_name|
        add_space_file(File.join(data_directory, file_name))
      end

      nil
    end

    def each_space(&block)
      return enum_for(:each_space) unless block_given?

      spaces.each_value(&block)

      nil
    end

    # Iterate throught all orphaned spaces.
    def each_orphan(&block)
      return enum_for(:each_orphan) unless block_given?

      orphans.each(&block)

      nil
    end

    # Return an Innodb::Index object given a table name and index name.
    def index_by_name(table_name, index_name)
      table = data_dictionary.tables.find(name: table_name)
      index = table.indexes.find(name: index_name)

      space(index.tablespace.innodb_space_id).index(index.root_page_number, index.record_describer)
    end

    # Return the clustered index given a table ID.
    def clustered_index_by_table_id(table_id)
      table = data_dictionary.tables.find(innodb_table_id: table_id)
      return unless table

      index_by_name(table.name, table.clustered_index.name)
    end

    def history
      Innodb::History.new(self)
    end
  end
end
