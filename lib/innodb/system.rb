# -*- encoding : utf-8 -*-

# A class representing an entire InnoDB system, having a system tablespace
# and any number of attached single-table tablespaces.
class Innodb::System
  # A hash of configuration options by configuration key.
  attr_reader :config

  # A hash of spaces by space ID.
  attr_reader :spaces

  # The Innodb::DataDictionary for this system.
  attr_reader :data_dictionary

  # A hash of orphaned spaces
  attr_reader :orphans

  # The space ID of the system space, always 0.
  SYSTEM_SPACE_ID = 0

  def initialize(system_space_file)
    @spaces = {}
    @orphans = []
    @config = {
      :datadir => File.dirname(system_space_file),
    }

    add_space_file(system_space_file)

    @data_dictionary = Innodb::DataDictionary.new(system_space)
  end

  # A helper to get the system space.
  def system_space
    spaces[SYSTEM_SPACE_ID]
  end

  # Add an already-constructed Innodb::Space object.
  def add_space(space)
    unless space.is_a?(Innodb::Space)
      raise "Object was not an Innodb::Space"
    end

    spaces[space.space_id.to_i] = space
  end

  # Add an orphaned space file
  def add_orphan(space_file)
    orphans << space_file
  end

  # Add a space by filename.
  def add_space_file(space_file)
    if File.exist?(space_file)
      space = Innodb::Space.new(space_file)
      space.innodb_system = self
      add_space(space)
    else
      add_orphan(space_file)
    end
  end

  # Add a space by table name, constructing an appropriate filename
  # from the provided table name.
  def add_table(table_name)
    add_space_file("%s/%s.ibd" % [config[:datadir], table_name])
  end

  # Return an Innodb::Space object for a given space ID, looking up
  # and adding the single-table space if necessary.
  def space(space_id)
    return spaces[space_id] if spaces[space_id]

    unless table_record = data_dictionary.table_by_space_id(space_id)
      raise "Table with space ID #{space_id} not found"
    end

    add_table(table_record["NAME"])

    spaces[space_id]
  end

  # Return an Innodb::Space object by table name.
  def space_by_table_name(table_name)
    unless table_record = data_dictionary.table_by_name(table_name)
      raise "Table #{table_name} not found"
    end

    if table_record["SPACE"] == 0
      return nil
    end

    space(table_record["SPACE"])
  end

  # Iterate through all table names.
  def each_table_name
    unless block_given?
      return enum_for(:each_table_name)
    end

    data_dictionary.each_table do |record|
      yield record["NAME"]
    end

    nil
  end

  # Iterate throught all orphaned spaces.
  def each_orphans
    orphans.each do |table|
      yield table
    end

    nil
  end

  # Iterate through all column names by table name.
  def each_column_name_by_table_name(table_name)
    unless block_given?
      return enum_for(:each_column_name_by_table_name, table_name)
    end

    data_dictionary.each_column_by_table_name(table_name) do |record|
      yield record["NAME"]
    end

    nil
  end

  # Iterate through all index names by table name.
  def each_index_name_by_table_name(table_name)
    unless block_given?
      return enum_for(:each_index_name_by_table_name, table_name)
    end

    data_dictionary.each_index_by_table_name(table_name) do |record|
      yield record["NAME"]
    end

    nil
  end

  # Iterate through all field names in a given index by table name
  # and index name.
  def each_index_field_name_by_index_name(table_name, index_name)
    unless block_given?
      return enum_for(:each_index_field_name_by_index_name,
                      table_name, index_name)
    end

    data_dictionary.each_field_by_index_name(table_name, index_name) do |record|
      yield record["COL_NAME"]
    end

    nil
  end

  # Return the table name given a table ID.
  def table_name_by_id(table_id)
    if table_record = data_dictionary.table_by_id(table_id)
      table_record["NAME"]
    end
  end

  # Return the index name given an index ID.
  def index_name_by_id(index_id)
    if index_record = data_dictionary.index_by_id(index_id)
      index_record["NAME"]
    end
  end

  # Return an array of the table name and index name given an index ID.
  def table_and_index_name_by_id(index_id)
    if index_record = data_dictionary.index_by_id(index_id)
      [table_name_by_id(index_record["TABLE_ID"]), index_record["NAME"]]
    end
  end

  # Return an Innodb::Index object given a table name and index name.
  def index_by_name(table_name, index_name)
    index_record = data_dictionary.index_by_name(table_name, index_name)

    index_space = space(index_record["SPACE"])
    describer = data_dictionary.record_describer_by_index_name(table_name, index_name)
    index = index_space.index(index_record["PAGE_NO"], describer)

    index
  end

  def history
    Innodb::History.new(self)
  end
end
