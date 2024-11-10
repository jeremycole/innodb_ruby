# frozen_string_literal: true

# A class representing MySQL's SDI-based data dictionary (used in MySQL
# versions starting in MySQL 8.0), which contains metadata about tables,
# columns, and indexes distributed in BLOBs of JSON stored in each InnoDB
# tablespace file.
module Innodb
  class SdiDataDictionary
    extend Forwardable

    attr_reader :innodb_system

    def_delegator :innodb_system, :data_dictionary

    def initialize(innodb_system)
      @innodb_system = innodb_system
    end

    def populate_data_dictionary_using_space_sdi(space)
      sdi_tablespace = space.sdi.tablespaces.first
      return unless sdi_tablespace

      innodb_space_id = sdi_tablespace.se_private_data["id"].to_i
      new_tablespace = data_dictionary.tablespaces.make(name: sdi_tablespace.name, innodb_space_id: innodb_space_id)

      space.sdi.tables.each do |table|
        new_table = data_dictionary.tables.make(name: table.name, tablespace: new_tablespace)

        table.columns.each do |column|
          next if %w[DB_TRX_ID DB_ROLL_PTR].include?(column.name)

          new_table.columns.make(name: column.name, description: column.description, table: new_table)
        end

        table.indexes.each do |index|
          new_index = new_table.indexes.make(
            name: index.name,
            type: index.clustered? ? :clustered : :secondary,
            table: new_table,
            tablespace: new_tablespace,
            root_page_number: index.root_page_number,
            innodb_index_id: index.innodb_index_id
          )

          db_trx_id = Innodb::DataDictionary::Column.new(
            name: "DB_TRX_ID",
            description: %i[DB_TRX_ID],
            table: new_table
          )
          db_roll_ptr = Innodb::DataDictionary::Column.new(
            name: "DB_ROLL_PTR",
            description: %i[DB_ROLL_PTR],
            table: new_table
          )

          index.elements.each do |element|
            case element.column.name
            when "DB_TRX_ID"
              new_index.column_references.make(column: db_trx_id, usage: :sys, index: new_index)
            when "DB_ROLL_PTR"
              new_index.column_references.make(column: db_roll_ptr, usage: :sys, index: new_index)
            else
              new_index.column_references.make(column: new_table.columns.find(name: element.column.name),
                                               usage: element.type, index: new_index)
            end
          end
        end
      end

      nil
    end

    def populate_data_dictionary
      data_dictionary.tablespaces.make(name: "innodb_system", innodb_space_id: 0)

      innodb_system.each_space do |space|
        populate_data_dictionary_using_space_sdi(space)
      end
    end
  end
end
