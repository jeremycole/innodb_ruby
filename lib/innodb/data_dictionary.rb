# -*- encoding : utf-8 -*-

# A class representing InnoDB's data dictionary, which contains metadata about
# tables, columns, and indexes.
class Innodb::DataDictionary
  # A record describer for SYS_TABLES clustered records.
  class SYS_TABLES_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "NAME",         "VARCHAR(100)",      :NOT_NULL
    row "ID",           :BIGINT, :UNSIGNED,  :NOT_NULL
    row "N_COLS",       :INT,    :UNSIGNED,  :NOT_NULL
    row "TYPE",         :INT,    :UNSIGNED,  :NOT_NULL
    row "MIX_ID",       :BIGINT, :UNSIGNED,  :NOT_NULL
    row "MIX_LEN",      :INT,    :UNSIGNED,  :NOT_NULL
    row "CLUSTER_NAME", "VARCHAR(100)",      :NOT_NULL
    row "SPACE",        :INT,    :UNSIGNED,  :NOT_NULL
  end

  # A record describer for SYS_TABLES secondary key on ID.
  class SYS_TABLES_ID < Innodb::RecordDescriber
    type :secondary
    key "ID",           :BIGINT, :UNSIGNED,  :NOT_NULL
    row "NAME",         "VARCHAR(100)",      :NOT_NULL
  end

  # A record describer for SYS_COLUMNS clustered records.
  class SYS_COLUMNS_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "TABLE_ID",     :BIGINT, :UNSIGNED,  :NOT_NULL
    key "POS",          :INT,    :UNSIGNED,  :NOT_NULL
    row "NAME",         "VARCHAR(100)",      :NOT_NULL
    row "MTYPE",        :INT,    :UNSIGNED,  :NOT_NULL
    row "PRTYPE",       :INT,    :UNSIGNED,  :NOT_NULL
    row "LEN",          :INT,    :UNSIGNED,  :NOT_NULL
    row "PREC",         :INT,    :UNSIGNED,  :NOT_NULL
  end

  # A record describer for SYS_INDEXES clustered records.
  class SYS_INDEXES_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "TABLE_ID",     :BIGINT, :UNSIGNED,  :NOT_NULL
    key "ID",           :BIGINT, :UNSIGNED,  :NOT_NULL
    row "NAME",         "VARCHAR(100)",      :NOT_NULL
    row "N_FIELDS",     :INT,    :UNSIGNED,  :NOT_NULL
    row "TYPE",         :INT,    :UNSIGNED,  :NOT_NULL
    row "SPACE",        :INT,    :UNSIGNED,  :NOT_NULL
    row "PAGE_NO",      :INT,    :UNSIGNED,  :NOT_NULL
  end

  # A record describer for SYS_FIELDS clustered records.
  class SYS_FIELDS_PRIMARY < Innodb::RecordDescriber
    type :clustered
    key "INDEX_ID",     :BIGINT, :UNSIGNED,  :NOT_NULL
    key "POS",          :INT,    :UNSIGNED,  :NOT_NULL
    row "COL_NAME",     "VARCHAR(100)",      :NOT_NULL
  end

  # A hash of hashes of table name and index name to describer
  # class.
  DATA_DICTIONARY_RECORD_DESCRIBERS = {
    :SYS_TABLES  => {
      :PRIMARY => SYS_TABLES_PRIMARY,
      :ID      => SYS_TABLES_ID
    },
    :SYS_COLUMNS => { :PRIMARY => SYS_COLUMNS_PRIMARY },
    :SYS_INDEXES => { :PRIMARY => SYS_INDEXES_PRIMARY },
    :SYS_FIELDS  => { :PRIMARY => SYS_FIELDS_PRIMARY },
  }

  # A hash of MySQL's internal type system to the stored
  # values for those types, and the "external" SQL type.
  MYSQL_TYPE = {
  # :DECIMAL      => { :value => 0,   :type => :DECIMAL     },
    :TINY         => { :value => 1,   :type => :TINYINT     },
    :SHORT        => { :value => 2,   :type => :SMALLINT    },
    :LONG         => { :value => 3,   :type => :INT         },
    :FLOAT        => { :value => 4,   :type => :FLOAT       },
    :DOUBLE       => { :value => 5,   :type => :DOUBLE      },
  # :NULL         => { :value => 6,   :type => nil          },
    :TIMESTAMP    => { :value => 7,   :type => :TIMESTAMP   },
    :LONGLONG     => { :value => 8,   :type => :BIGINT      },
    :INT24        => { :value => 9,   :type => :MEDIUMINT   },
  # :DATE         => { :value => 10,  :type => :DATE        },
    :TIME         => { :value => 11,  :type => :TIME        },
    :DATETIME     => { :value => 12,  :type => :DATETIME    },
    :YEAR         => { :value => 13,  :type => :YEAR        },
    :NEWDATE      => { :value => 14,  :type => :DATE        },
    :VARCHAR      => { :value => 15,  :type => :VARCHAR     },
    :BIT          => { :value => 16,  :type => :BIT         },
    :NEWDECIMAL   => { :value => 246, :type => :CHAR        },
  # :ENUM         => { :value => 247, :type => :ENUM        },
  # :SET          => { :value => 248, :type => :SET         },
    :TINY_BLOB    => { :value => 249, :type => :TINYBLOB    },
    :MEDIUM_BLOB  => { :value => 250, :type => :MEDIUMBLOB  },
    :LONG_BLOB    => { :value => 251, :type => :LONGBLOB    },
    :BLOB         => { :value => 252, :type => :BLOB        },
  # :VAR_STRING   => { :value => 253, :type => :VARCHAR     },
    :STRING       => { :value => 254, :type => :CHAR        },
    :GEOMETRY     => { :value => 255, :type => :GEOMETRY    },
  }

  # A hash of MYSQL_TYPE keys by value :value key.
  MYSQL_TYPE_BY_VALUE = MYSQL_TYPE.inject({}) { |h, (k, v)| h[v[:value]] = k; h }

  # A hash of InnoDB's internal type system to the values
  # stored for each type.
  COLUMN_MTYPE = {
    :VARCHAR    => 1,
    :CHAR       => 2,
    :FIXBINARY  => 3,
    :BINARY     => 4,
    :BLOB       => 5,
    :INT        => 6,
    :SYS_CHILD  => 7,
    :SYS        => 8,
    :FLOAT      => 9,
    :DOUBLE     => 10,
    :DECIMAL    => 11,
    :VARMYSQL   => 12,
    :MYSQL      => 13,
  }

  # A hash of COLUMN_MTYPE keys by value.
  COLUMN_MTYPE_BY_VALUE = COLUMN_MTYPE.inject({}) { |h, (k, v)| h[v] = k; h }

  # A hash of InnoDB "precise type" bitwise flags.
  COLUMN_PRTYPE_FLAG = {
    :NOT_NULL          => 256,
    :UNSIGNED          => 512,
    :BINARY            => 1024,
    :LONG_TRUE_VARCHAR => 4096,
  }

  # A hash of COLUMN_PRTYPE keys by value.
  COLUMN_PRTYPE_FLAG_BY_VALUE = COLUMN_PRTYPE_FLAG.inject({}) { |h, (k, v)| h[v] = k; h }

  # The bitmask to extract the MySQL internal type
  # from the InnoDB "precise type".
  COLUMN_PRTYPE_MYSQL_TYPE_MASK = 0xFF

  # A hash of InnoDB's index type flags.
  INDEX_TYPE_FLAG = {
    :CLUSTERED => 1,
    :UNIQUE    => 2,
    :UNIVERSAL => 4,
    :IBUF      => 8,
    :CORRUPT   => 16,
    :FTS       => 32,
  }

  # A hash of INDEX_TYPE_FLAG keys by value.
  INDEX_TYPE_FLAG_BY_VALUE = INDEX_TYPE_FLAG.inject({}) { |h, (k, v)| h[v] = k; h }

  # Return the "external" SQL type string (such as "VARCHAR" or
  # "INT") given the stored mtype and prtype from the InnoDB
  # data dictionary. Note that not all types are extractable
  # into fully defined SQL types due to the lossy nature of
  # the MySQL-to-InnoDB interface regarding types.
  def self.mtype_prtype_to_type_string(mtype, prtype, len, prec)
    mysql_type = prtype & COLUMN_PRTYPE_MYSQL_TYPE_MASK
    internal_type = MYSQL_TYPE_BY_VALUE[mysql_type]
    external_type = MYSQL_TYPE[internal_type][:type]

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
  # as ["VARCHAR(10)", :NOT_NULL] or [:INT, :UNSIGNED].
  def self.mtype_prtype_to_data_type(mtype, prtype, len, prec)
    data_type = []

    if type = mtype_prtype_to_type_string(mtype, prtype, len, prec)
      data_type << type
    else
      raise "Unsupported type (mtype #{mtype}, prtype #{prtype})"
    end

    if prtype & COLUMN_PRTYPE_FLAG[:NOT_NULL] != 0
      data_type << :NOT_NULL
    end

    if prtype & COLUMN_PRTYPE_FLAG[:UNSIGNED] != 0
      data_type << :UNSIGNED
    end

    data_type
  end

  attr_reader :system_space

  def initialize(system_space)
    @system_space = system_space
  end

  # A helper method to reach inside the system space and retrieve
  # the data dictionary index locations from the data dictionary
  # header.
  def data_dictionary_indexes
    system_space.data_dictionary_page.data_dictionary_header[:indexes]
  end

  def data_dictionary_index_ids
    if @data_dictionary_index_ids
      return @data_dictionary_index_ids
    end

    @data_dictionary_index_ids = {}
    data_dictionary_indexes.each do |table, indexes|
      indexes.each do |index, root_page_number|
        if root_page = system_space.page(root_page_number)
          @data_dictionary_index_ids[root_page.index_id] = {
            :table => table,
            :index => index
          }
        end
      end
    end

    @data_dictionary_index_ids
  end

  def data_dictionary_table?(table_name)
    DATA_DICTIONARY_RECORD_DESCRIBERS.include?(table_name.to_sym)
  end

  def data_dictionary_index?(table_name, index_name)
    return unless data_dictionary_table?(table_name)
    DATA_DICTIONARY_RECORD_DESCRIBERS[table_name.to_sym].include?(index_name.to_sym)
  end

  def data_dictionary_index_describer(table_name, index_name)
    return unless data_dictionary_index?(table_name, index_name)

    DATA_DICTIONARY_RECORD_DESCRIBERS[table_name.to_sym][index_name.to_sym].new
  end

  # Return an Innodb::Index object initialized to the
  # internal data dictionary index with an appropriate
  # record describer so that records can be recursed.
  def data_dictionary_index(table_name, index_name)
    unless table_entry = data_dictionary_indexes[table_name]
      raise "Unknown data dictionary table #{table_name}"
    end

    unless index_root_page = table_entry[index_name]
      raise "Unknown data dictionary index #{table_name}.#{index_name}"
    end

    # If we have a record describer for this index, load it.
    record_describer = data_dictionary_index_describer(table_name, index_name)

    system_space.index(index_root_page, record_describer)
  end

  # Iterate through all data dictionary indexes, yielding the
  # table name, index name, and root page number.
  def each_data_dictionary_index_root_page_number
    unless block_given?
      return enum_for(:each_data_dictionary_index_root_page_number)
    end

    data_dictionary_indexes.each do |table_name, indexes|
      indexes.each do |index_name, root_page_number|
        yield table_name, index_name, root_page_number
      end
    end

    nil
  end

  # Iterate through all data dictionary indexes, yielding the table
  # name, index name, and the index itself as an Innodb::Index.
  def each_data_dictionary_index
    unless block_given?
      return enum_for(:each_data_dictionary_index)
    end

    data_dictionary_indexes.each do |table_name, indexes|
      indexes.each do |index_name, root_page_number|
        yield table_name, index_name,
          data_dictionary_index(table_name, index_name)
      end
    end

    nil
  end

  # Iterate through records from a data dictionary index yielding each record
  # as a Innodb::Record object.
  def each_record_from_data_dictionary_index(table, index)
    unless block_given?
      return enum_for(:each_index, table, index)
    end

    data_dictionary_index(table, index).each_record do |record|
      yield record
    end

    nil
  end

  # Iterate through the records in the SYS_TABLES data dictionary table.
  def each_table
    unless block_given?
      return enum_for(:each_table)
    end

    each_record_from_data_dictionary_index(:SYS_TABLES, :PRIMARY) do |record|
      yield record.fields
    end

    nil
  end

  # Iterate through the records in the SYS_COLUMNS data dictionary table.
  def each_column
    unless block_given?
      return enum_for(:each_column)
    end

    each_record_from_data_dictionary_index(:SYS_COLUMNS, :PRIMARY) do |record|
      yield record.fields
    end

    nil
  end

  # Iterate through the records in the SYS_INDEXES dictionary table.
  def each_index
    unless block_given?
      return enum_for(:each_index)
    end

    each_record_from_data_dictionary_index(:SYS_INDEXES, :PRIMARY) do |record|
      yield record.fields
    end

    nil
  end

  # Iterate through the records in the SYS_FIELDS data dictionary table.
  def each_field
    unless block_given?
      return enum_for(:each_field)
    end

    each_record_from_data_dictionary_index(:SYS_FIELDS, :PRIMARY) do |record|
      yield record.fields
    end

    nil
  end

  # A helper to iterate the method provided and return the first record
  # where the record's field matches the provided value.
  def object_by_field(method, field, value)
    send(method).select { |o| o[field] == value }.first
  end

  # A helper to iterate the method provided and return the first record
  # where the record's fields f1 and f2 match the provided values v1 and v2.
  def object_by_two_fields(method, f1, v1, f2, v2)
    send(method).select { |o| o[f1] == v1 && o[f2] == v2 }.first
  end

  # Lookup a table by table ID.
  def table_by_id(table_id)
    object_by_field(:each_table,
                    "ID", table_id)
  end

  # Lookup a table by table name.
  def table_by_name(table_name)
    object_by_field(:each_table,
                    "NAME", table_name)
  end

  # Lookup a table by space ID.
  def table_by_space_id(space_id)
    object_by_field(:each_table,
                    "SPACE", space_id)
  end

  # Lookup a column by table name and column name.
  def column_by_name(table_name, column_name)
    unless table = table_by_name(table_name)
      return nil
    end

    object_by_two_fields(:each_column,
                         "TABLE_ID", table["ID"],
                         "NAME", column_name)
  end

  # Lookup an index by index ID.
  def index_by_id(index_id)
    object_by_field(:each_index,
                    "ID", index_id)
  end

  # Lookup an index by table name and index name.
  def index_by_name(table_name, index_name)
    unless table = table_by_name(table_name)
      return nil
    end

    object_by_two_fields(:each_index,
                         "TABLE_ID", table["ID"],
                         "NAME", index_name)
  end

  # Iterate through indexes by space ID.
  def each_index_by_space_id(space_id)
    unless block_given?
      return enum_for(:each_index_by_space_id, space_id)
    end

    each_index do |record|
      yield record if record["SPACE"] == space_id
    end

    nil
  end

  # Iterate through all indexes in a table by table ID.
  def each_index_by_table_id(table_id)
    unless block_given?
      return enum_for(:each_index_by_table_id, table_id)
    end

    each_index do |record|
      yield record if record["TABLE_ID"] == table_id
    end

    nil
  end

  # Iterate through all indexes in a table by table name.
  def each_index_by_table_name(table_name)
    unless block_given?
      return enum_for(:each_index_by_table_name, table_name)
    end

    unless table = table_by_name(table_name)
      raise "Table #{table_name} not found"
    end

    each_index_by_table_id(table["ID"]) do |record|
      yield record
    end

    nil
  end

  # Iterate through all fields in an index by index ID.
  def each_field_by_index_id(index_id)
    unless block_given?
      return enum_for(:each_field_by_index_id, index_id)
    end

    each_field do |record|
      yield record if record["INDEX_ID"] == index_id
    end

    nil
  end

  # Iterate through all fields in an index by index name.
  def each_field_by_index_name(table_name, index_name)
    unless block_given?
      return enum_for(:each_field_by_name, table_name, index_name)
    end

    unless index = index_by_name(table_name, index_name)
      raise "Index #{index_name} for table #{table_name} not found"
    end

    each_field_by_index_id(index["ID"]) do |record|
      yield record
    end

    nil
  end

  # Iterate through all columns in a table by table ID.
  def each_column_by_table_id(table_id)
    unless block_given?
      return enum_for(:each_column_by_table_id, table_id)
    end

    each_column do |record|
      yield record if record["TABLE_ID"] == table_id
    end

    nil
  end

  # Iterate through all columns in a table by table name.
  def each_column_by_table_name(table_name)
    unless block_given?
      return enum_for(:each_column_by_table_name, table_name)
    end

    unless table = table_by_name(table_name)
      raise "Table #{table_name} not found"
    end

    each_column_by_table_id(table["ID"]) do |record|
      yield record
    end

    nil
  end

  # Iterate through all columns in an index by table name and index name.
  def each_column_in_index_by_name(table_name, index_name)
    unless block_given?
      return enum_for(:each_column_in_index_by_name, table_name, index_name)
    end

    each_field_by_index_name(table_name, index_name) do |record|
      yield column_by_name(table_name, record["COL_NAME"])
    end

    nil
  end

  # Iterate through all columns not in an index by table name and index name.
  # This is useful when building index descriptions for secondary indexes.
  def each_column_not_in_index_by_name(table_name, index_name)
    unless block_given?
      return enum_for(:each_column_not_in_index_by_name, table_name, index_name)
    end

    columns_in_index = {}
    each_column_in_index_by_name(table_name, index_name) do |record|
      columns_in_index[record["NAME"]] = 1
    end

    each_column_by_table_name(table_name) do |record|
      yield record unless columns_in_index.include?(record["NAME"])
    end

    nil
  end

  # Return the name of the clustered index (usually "PRIMARY", but not always)
  # for a given table name.
  def clustered_index_name_by_table_name(table_name)
    unless table_record = table_by_name(table_name)
      raise "Table #{table_name} not found"
    end

    if index_record = object_by_two_fields(:each_index,
                                           "TABLE_ID", table_record["ID"],
                                           "TYPE", 3)
      index_record["NAME"]
    end
  end

  # Produce a Innodb::RecordDescriber-compatible column description
  # given a type (:key, :row) and data dictionary SYS_COLUMNS record.
  def _make_column_description(type, record)
    {
      :type => type,
      :name => record["NAME"],
      :description => self.class.mtype_prtype_to_data_type(
        record["MTYPE"],
        record["PRTYPE"],
        record["LEN"],
        record["PREC"]
      )
    }
  end

  # Iterate through Innodb::RecordDescriber-compatible column descriptions
  # for a given index by table name and index name.
  def each_column_description_by_index_name(table_name, index_name)
    unless block_given?
      return enum_for(:each_column_description_by_index_name, table_name, index_name)
    end

    unless index = index_by_name(table_name, index_name)
      raise "Index #{index_name} for table #{table_name} not found"
    end

    columns_in_index = {}
    each_column_in_index_by_name(table_name, index_name) do |record|
      columns_in_index[record["NAME"]] = 1
      yield _make_column_description(:key, record)
    end

    if index["TYPE"] & INDEX_TYPE_FLAG[:CLUSTERED] != 0
      each_column_by_table_name(table_name) do |record|
        unless columns_in_index.include?(record["NAME"])
          yield _make_column_description(:row, record)
        end
      end
    else
      clustered_index_name = clustered_index_name_by_table_name(table_name)

      each_column_in_index_by_name(table_name, clustered_index_name) do |record|
        yield _make_column_description(:row, record)
      end
    end

    nil
  end

  # Return an Innodb::RecordDescriber object describing records for a given
  # index by table name and index name.
  def record_describer_by_index_name(table_name, index_name)
    if data_dictionary_index?(table_name, index_name)
      return data_dictionary_index_describer(table_name, index_name)
    end

    unless index = index_by_name(table_name, index_name)
      raise "Index #{index_name} for table #{table_name} not found"
    end

    describer = Innodb::RecordDescriber.new

    if index["TYPE"] & INDEX_TYPE_FLAG[:CLUSTERED] != 0
      describer.type :clustered
    else
      describer.type :secondary
    end

    each_column_description_by_index_name(table_name, index_name) do |column|
      case column[:type]
      when :key
        describer.key column[:name], *column[:description]
      when :row
        describer.row column[:name], *column[:description]
      end
    end

    describer
  end

  # Return an Innodb::RecordDescriber object describing the records
  # in a given index by index ID.
  def record_describer_by_index_id(index_id)
    if dd_index = data_dictionary_index_ids[index_id]
      return data_dictionary_index_describer(dd_index[:table], dd_index[:index])
    end

    unless index = index_by_id(index_id)
      raise "Index #{index_id} not found"
    end

    unless table = table_by_id(index["TABLE_ID"])
      raise "Table #{INDEX["TABLE_ID"]} not found"
    end

    record_describer_by_index_name(table["NAME"], index["NAME"])
  end

end
