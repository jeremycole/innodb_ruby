# frozen_string_literal: true

module Innodb
  class MysqlType
    attr_reader :mysql_field_type_value
    attr_reader :sdi_column_type_value
    attr_reader :type
    attr_reader :handle_as

    def initialize(type:, mysql_field_type_value:, sdi_column_type_value:, handle_as: nil)
      @mysql_field_type_value = mysql_field_type_value
      @sdi_column_type_value = sdi_column_type_value
      @type = type
      @handle_as = handle_as || type
    end

    # A hash of MySQL's internal type system to the stored
    # values for those types, and the 'external' SQL type.
    TYPES = [
      new(type: :DECIMAL, mysql_field_type_value: 0, sdi_column_type_value: 1),
      new(type: :TINYINT, mysql_field_type_value: 1, sdi_column_type_value: 2),
      new(type: :SMALLINT, mysql_field_type_value: 2, sdi_column_type_value: 3),
      new(type: :INT, mysql_field_type_value: 3, sdi_column_type_value: 4),
      new(type: :FLOAT, mysql_field_type_value: 4, sdi_column_type_value: 5),
      new(type: :DOUBLE, mysql_field_type_value: 5, sdi_column_type_value: 6),
      new(type: :TYPE_NULL, mysql_field_type_value: 6, sdi_column_type_value: 7),
      new(type: :TIMESTAMP, mysql_field_type_value: 7, sdi_column_type_value: 8),
      new(type: :BIGINT, mysql_field_type_value: 8, sdi_column_type_value: 9),
      new(type: :MEDIUMINT, mysql_field_type_value: 9, sdi_column_type_value: 10),
      new(type: :DATE, mysql_field_type_value: 10, sdi_column_type_value: 11),
      new(type: :TIME, mysql_field_type_value: 11, sdi_column_type_value: 12),
      new(type: :DATETIME, mysql_field_type_value: 12, sdi_column_type_value: 13),
      new(type: :YEAR, mysql_field_type_value: 13, sdi_column_type_value: 14),
      new(type: :DATE, mysql_field_type_value: 14, sdi_column_type_value: 15),
      new(type: :VARCHAR, mysql_field_type_value: 15, sdi_column_type_value: 16),
      new(type: :BIT, mysql_field_type_value: 16, sdi_column_type_value: 17),
      new(type: :TIMESTAMP2, mysql_field_type_value: 17, sdi_column_type_value: 18),
      new(type: :DATETIME2, mysql_field_type_value: 18, sdi_column_type_value: 19),
      new(type: :TIME2, mysql_field_type_value: 19, sdi_column_type_value: 20),
      new(type: :NEWDECIMAL, mysql_field_type_value: 246, sdi_column_type_value: 21, handle_as: :CHAR),
      new(type: :ENUM, mysql_field_type_value: 247, sdi_column_type_value: 22),
      new(type: :SET, mysql_field_type_value: 248, sdi_column_type_value: 23),
      new(type: :TINYBLOB, mysql_field_type_value: 249, sdi_column_type_value: 24),
      new(type: :MEDIUMBLOB, mysql_field_type_value: 250, sdi_column_type_value: 25),
      new(type: :LONGBLOB, mysql_field_type_value: 251, sdi_column_type_value: 26),
      new(type: :BLOB, mysql_field_type_value: 252, sdi_column_type_value: 27),
      new(type: :VARCHAR, mysql_field_type_value: 253, sdi_column_type_value: 28),
      new(type: :CHAR, mysql_field_type_value: 254, sdi_column_type_value: 29),
      new(type: :GEOMETRY, mysql_field_type_value: 255, sdi_column_type_value: 30),
      new(type: :JSON, mysql_field_type_value: 245, sdi_column_type_value: 31),
    ].freeze

    # A hash of types by mysql_field_type_value.
    TYPES_BY_MYSQL_FIELD_TYPE_VALUE = Innodb::MysqlType::TYPES.to_h { |t| [t.mysql_field_type_value, t] }.freeze

    # A hash of types by sdi_column_type_value.
    TYPES_BY_SDI_COLUMN_TYPE_VALUE = Innodb::MysqlType::TYPES.to_h { |t| [t.sdi_column_type_value, t] }.freeze

    def self.by_mysql_field_type(value)
      TYPES_BY_MYSQL_FIELD_TYPE_VALUE[value]
    end

    def self.by_sdi_column_type(value)
      TYPES_BY_SDI_COLUMN_TYPE_VALUE[value]
    end
  end
end
