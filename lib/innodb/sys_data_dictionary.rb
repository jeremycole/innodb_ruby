# frozen_string_literal: true

require "forwardable"

# A class representing InnoDB's SYS_* data dictionary (used in MySQL
# versions prior to MySQL 8.0), which contains metadata about tables,
# columns, and indexes in internal InnoDB tables named SYS_*.
module Innodb
  class SysDataDictionary
    # rubocop:disable Layout/ExtraSpacing
    SYSTEM_TABLES = [
      {
        name: "SYS_TABLES",
        columns: [
          { name: "NAME",         description: ["VARCHAR(100)",     :NOT_NULL] },
          { name: "ID",           description: %i[BIGINT UNSIGNED NOT_NULL] },
          { name: "N_COLS",       description: %i[INT UNSIGNED NOT_NULL] },
          { name: "TYPE",         description: %i[INT UNSIGNED NOT_NULL] },
          { name: "MIX_ID",       description: %i[BIGINT UNSIGNED NOT_NULL] },
          { name: "MIX_LEN",      description: %i[INT UNSIGNED NOT_NULL] },
          { name: "CLUSTER_NAME", description: ["VARCHAR(100)",     :NOT_NULL] },
          { name: "SPACE",        description: %i[INT UNSIGNED NOT_NULL] },
        ],
        indexes: [
          { name: "PRIMARY", type: :clustered, column_names: ["NAME"] },
          { name: "ID", type: :secondary, column_names: ["ID"] },
        ],
      },
      {
        name: "SYS_COLUMNS",
        columns: [
          { name: "TABLE_ID",     description: %i[BIGINT UNSIGNED NOT_NULL] },
          { name: "POS",          description: %i[INT UNSIGNED NOT_NULL] },
          { name: "NAME",         description: ["VARCHAR(100)",     :NOT_NULL] },
          { name: "MTYPE",        description: %i[INT UNSIGNED NOT_NULL] },
          { name: "PRTYPE",       description: %i[INT UNSIGNED NOT_NULL] },
          { name: "LEN",          description: %i[INT UNSIGNED NOT_NULL] },
          { name: "PREC",         description: %i[INT UNSIGNED NOT_NULL] },
        ],
        indexes: [
          { name: "PRIMARY", type: :clustered, column_names: %w[TABLE_ID POS] },
        ],
      },
      {
        name: "SYS_INDEXES",
        columns: [
          { name: "TABLE_ID",     description: %i[BIGINT UNSIGNED NOT_NULL] },
          { name: "ID",           description: %i[BIGINT UNSIGNED NOT_NULL] },
          { name: "NAME",         description: ["VARCHAR(100)",     :NOT_NULL] },
          { name: "N_FIELDS",     description: %i[INT UNSIGNED NOT_NULL] },
          { name: "TYPE",         description: %i[INT UNSIGNED NOT_NULL] },
          { name: "SPACE",        description: %i[INT UNSIGNED NOT_NULL] },
          { name: "PAGE_NO",      description: %i[INT UNSIGNED NOT_NULL] },
        ],
        indexes: [
          { name: "PRIMARY", type: :clustered, column_names: %w[TABLE_ID ID] },
        ],
      },
      {
        name: "SYS_FIELDS",
        columns: [
          { name: "INDEX_ID",     description: %i[BIGINT UNSIGNED NOT_NULL] },
          { name: "POS",          description: %i[INT UNSIGNED NOT_NULL] },
          { name: "COL_NAME",     description: ["VARCHAR(100)",     :NOT_NULL] },
        ],
        indexes: [
          { name: "PRIMARY", type: :clustered, column_names: %w[INDEX_ID POS] },
        ],
      },
    ].freeze
    # rubocop:enable Layout/ExtraSpacing

    # A hash of InnoDB's internal type system to the values
    # stored for each type.
    COLUMN_MTYPE = {
      VARCHAR: 1,
      CHAR: 2,
      FIXBINARY: 3,
      BINARY: 4,
      BLOB: 5,
      INT: 6,
      SYS_CHILD: 7,
      SYS: 8,
      FLOAT: 9,
      DOUBLE: 10,
      DECIMAL: 11,
      VARMYSQL: 12,
      MYSQL: 13,
    }.freeze

    # A hash of COLUMN_MTYPE keys by value.
    COLUMN_MTYPE_BY_VALUE = COLUMN_MTYPE.invert.freeze

    # A hash of InnoDB 'precise type' bitwise flags.
    COLUMN_PRTYPE_FLAG = {
      NOT_NULL: 256,
      UNSIGNED: 512,
      BINARY: 1024,
      LONG_TRUE_VARCHAR: 4096,
    }.freeze

    # A hash of COLUMN_PRTYPE keys by value.
    COLUMN_PRTYPE_FLAG_BY_VALUE = COLUMN_PRTYPE_FLAG.invert.freeze

    # The bitmask to extract the MySQL internal type
    # from the InnoDB 'precise type'.
    COLUMN_PRTYPE_MYSQL_TYPE_MASK = 0xFF

    # A hash of InnoDB's index type flags.
    INDEX_TYPE_FLAG = {
      CLUSTERED: 1,
      UNIQUE: 2,
      UNIVERSAL: 4,
      IBUF: 8,
      CORRUPT: 16,
      FTS: 32,
    }.freeze

    # A hash of INDEX_TYPE_FLAG keys by value.
    INDEX_TYPE_FLAG_BY_VALUE = INDEX_TYPE_FLAG.invert.freeze

    # Return the 'external' SQL type string (such as 'VARCHAR' or
    # 'INT') given the stored mtype and prtype from the InnoDB
    # data dictionary. Note that not all types are extractable
    # into fully defined SQL types due to the lossy nature of
    # the MySQL-to-InnoDB interface regarding types.
    def self.mtype_prtype_to_type_string(mtype, prtype, len, prec)
      mysql_type = prtype & COLUMN_PRTYPE_MYSQL_TYPE_MASK
      internal_type = Innodb::MysqlType.by_mysql_field_type(mysql_type)
      external_type = internal_type.handle_as

      case external_type
      when :VARCHAR
        # One-argument: length.
        "%s(%i)" % [external_type, len]
      when :FLOAT, :DOUBLE
        # Two-argument: length and precision.
        "%s(%i,%i)" % [external_type, len, prec]
      when :CHAR
        if COLUMN_MTYPE_BY_VALUE[mtype] == :MYSQL
          # When the mtype is :MYSQL, the column is actually
          # stored as VARCHAR despite being a CHAR. This is
          # done for CHAR columns having multi-byte character
          # sets in order to limit size. Note that such data
          # are still space-padded to at least len.
          "VARCHAR(%i)" % [len]
        else
          "CHAR(%i)" % [len]
        end
      when :DECIMAL
        # The DECIMAL type is designated as DECIMAL(M,D)
        # however the M and D definitions are not stored
        # in the InnoDB data dictionary. We need to define
        # the column as something which will extract the
        # raw bytes in order to read the column, but we
        # can't figure out the right decimal type. The
        # len stored here is actually the on-disk storage
        # size.
        "CHAR(%i)" % [len]
      else
        external_type
      end
    end

    # Return a full data type given an mtype and prtype, such
    # as ['VARCHAR(10)', :NOT_NULL] or [:INT, :UNSIGNED].
    def self.mtype_prtype_to_data_type(mtype, prtype, len, prec)
      type = mtype_prtype_to_type_string(mtype, prtype, len, prec)
      raise "Unsupported type (mtype #{mtype}, prtype #{prtype})" unless type

      data_type = [type]
      data_type << :NOT_NULL if prtype & COLUMN_PRTYPE_FLAG[:NOT_NULL] != 0
      data_type << :UNSIGNED if prtype & COLUMN_PRTYPE_FLAG[:UNSIGNED] != 0

      data_type
    end

    extend Forwardable

    attr_reader :innodb_system

    def_delegator :innodb_system, :data_dictionary

    def initialize(innodb_system)
      @innodb_system = innodb_system
    end

    private

    # A helper method to reach inside the system space and retrieve
    # the data dictionary index locations from the data dictionary
    # header.
    def _data_dictionary_indexes
      innodb_system.system_space.data_dictionary_page.data_dictionary_header[:indexes]
    end

    def _populate_index_with_system_and_non_key_columns(new_table, new_index)
      if new_index.type == :clustered
        db_trx_id = Innodb::DataDictionary::Column.new(name: "DB_TRX_ID", description: %i[DB_TRX_ID], table: new_table)
        new_index.column_references.make(column: db_trx_id, usage: :sys, index: new_index)
        db_roll_ptr = Innodb::DataDictionary::Column.new(name: "DB_ROLL_PTR", description: %i[DB_ROLL_PTR],
                                                         table: new_table)
        new_index.column_references.make(column: db_roll_ptr, usage: :sys, index: new_index)

        new_table.columns.each do |column|
          unless new_index.column_references.find(name: column.name)
            new_index.column_references.make(column: column, usage: :row,
                                             index: new_index)
          end
        end
      else
        clustered_index = new_table.indexes.find(type: :clustered)
        clustered_index.column_references.each do |column|
          new_index.column_references.make(column: column, usage: :row, index: new_index) unless column.usage == :sys
        end
      end
    end

    public

    def populate_data_dictionary_with_system_table_definitions
      system_tablespace = data_dictionary.tablespaces.make(name: "innodb_system", innodb_space_id: 0)

      SYSTEM_TABLES.each do |table|
        new_table = data_dictionary.tables.make(name: table[:name], tablespace: system_tablespace)

        table[:columns].each do |column|
          new_table.columns.make(name: column[:name], description: column[:description], table: new_table)
        end

        table[:indexes].each do |index|
          new_index = new_table.indexes.make(
            name: index[:name],
            type: index[:type],
            table: new_table,
            tablespace: system_tablespace,
            root_page_number: _data_dictionary_indexes[table[:name]][index[:name]]
          )
          index[:column_names].each do |column_name|
            new_index.column_references.make(
              column: new_table.columns.find(name: column_name),
              usage: :key,
              index: new_index
            )
          end
          _populate_index_with_system_and_non_key_columns(new_table, new_index)
        end
      end

      nil
    end

    def populate_data_dictionary_from_system_tables
      # Read the entire contents of all tables for efficiency sake, since we'll need to do many sub-iterations
      # below and don't want to re-parse the records every time.
      sys_tables = innodb_system.index_by_name("SYS_TABLES", "PRIMARY").each_record.map(&:fields)
      sys_columns = innodb_system.index_by_name("SYS_COLUMNS", "PRIMARY").each_record.map(&:fields)
      sys_indexes = innodb_system.index_by_name("SYS_INDEXES", "PRIMARY").each_record.map(&:fields)
      sys_fields = innodb_system.index_by_name("SYS_FIELDS", "PRIMARY").each_record.map(&:fields)

      sys_tables.each do |table_record|
        tablespace = data_dictionary.tablespaces.find(innodb_space_id: table_record["SPACE"])
        tablespace ||= data_dictionary.tablespaces.make(name: table_record["NAME"],
                                                        innodb_space_id: table_record["SPACE"])

        new_table = data_dictionary.tables.make(name: table_record["NAME"], tablespace: tablespace,
                                                innodb_table_id: table_record["ID"])

        sys_columns.select { |r| r["TABLE_ID"] == table_record["ID"] }.each do |column_record|
          description = self.class.mtype_prtype_to_data_type(
            column_record["MTYPE"],
            column_record["PRTYPE"],
            column_record["LEN"],
            column_record["PREC"]
          )
          new_table.columns.make(name: column_record["NAME"], description: description, table: new_table)
        end

        sys_indexes.select { |r| r["TABLE_ID"] == table_record["ID"] }.each do |index_record|
          raise "Different tablespace between table and index" unless table_record["SPACE"] == index_record["SPACE"]

          type = index_record["TYPE"] & INDEX_TYPE_FLAG[:CLUSTERED] ? :clustered : :secondary
          new_index = new_table.indexes.make(
            name: index_record["NAME"],
            type: type,
            table: new_table,
            tablespace: tablespace,
            root_page_number: index_record["PAGE_NO"],
            innodb_index_id: index_record["ID"]
          )
          sys_fields.select { |r| r["INDEX_ID"] == index_record["ID"] }.each do |field_record|
            new_index.column_references.make(column: new_table.columns.find(name: field_record["COL_NAME"]),
                                             usage: :key, index: new_index)
          end

          _populate_index_with_system_and_non_key_columns(new_table, new_index)
        end
      end

      nil
    end

    def populate_data_dictionary
      populate_data_dictionary_with_system_table_definitions
      populate_data_dictionary_from_system_tables

      nil
    end
  end
end
