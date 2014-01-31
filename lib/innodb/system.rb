# -*- encoding : utf-8 -*-

class Innodb::System
  attr_reader :config
  attr_accessor :spaces

  def initialize(system_space_file)
    @config = {
      :datadir => ".",
    }
    @spaces = {}
    @spaces[0] = Innodb::Space.new(system_space_file)
  end

  def system_space
    @spaces[0]
  end

  def each_record_from_data_dictionary_index(table, index)
    unless block_given?
      return enum_for(:each_record_from_data_dictionary_index, table, index)
    end

    index = system_space.data_dictionary.index(table, index)
    index.each_record do |record|
      yield record
    end
    nil
  end

  def each_table
    unless block_given?
      return enum_for(:each_table)
    end

    each_record_from_data_dictionary_index(:SYS_TABLES, :PRIMARY) do |record|
      yield record.fields
    end
  end

  def each_index
    unless block_given?
      return enum_for(:each_index)
    end

    each_record_from_data_dictionary_index(:SYS_INDEXES, :PRIMARY) do |record|
      yield record.fields
    end
  end

  def add_space(space)
    @spaces[space.space_id] = space
  end

  def add_space_file(space_file)
    add_space(Innodb::Space.new(space_file))
  end
end
