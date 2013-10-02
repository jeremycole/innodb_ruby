# -*- encoding : utf-8 -*-
class Innodb::DataType
  class GenericType
    attr_reader :data_type

    def initialize(data_type)
      @data_type = data_type
    end
  end

  class IntegerType < GenericType
    def value(data)
      nbits = @data_type.length * 8
      @data_type.unsigned? ? get_uint(data, nbits) : get_int(data, nbits)
    end

    def get_uint(data, nbits)
      BinData::const_get("Uint%dbe" % nbits).read(data)
    end

    def get_int(data, nbits)
      BinData::const_get("Int%dbe" % nbits).read(data) ^ (-1 << (nbits - 1))
    end
  end

  class VariableStringType < GenericType
    def value(data)
      # The SQL standard defines that VARCHAR fields should have end-spaces
      # stripped off.
      data.sub(/[ ]+$/, "")
    end
  end

  # Maps data type to fixed storage length.
  TYPES = {
    :TINYINT    => { :class => IntegerType, :length => 1 },
    :SMALLINT   => { :class => IntegerType, :length => 2 },
    :MEDIUMINT  => { :class => IntegerType, :length => 3 },
    :INT        => { :class => IntegerType, :length => 4 },
    :INT6       => { :class => IntegerType, :length => 6 },
    :BIGINT     => { :class => IntegerType, :length => 8 },
    :CHAR       => { :class => GenericType },
    :VARCHAR    => { :class => VariableStringType },
    :BLOB       => { :class => GenericType },
  }

  def self.parse_base_type_and_modifiers(type_string)
    if matches = /^([a-zA-Z0-9]+)(\(([0-9, ]+)\))?$/.match(type_string)
      base_type = matches[1].upcase.to_sym
      if matches[3]
        modifiers = matches[3].sub(/[ ]/, "").split(/,/).map { |s| s.to_i }
      else
        modifiers = []
      end
      [base_type, modifiers]
    end
  end

  attr_reader :base_type
  attr_reader :length
  attr_reader :reader
  def initialize(type_string, properties)
    @base_type, modifiers = self.class.parse_base_type_and_modifiers(type_string)
    @length = nil
    @unsigned = properties.include?(:UNSIGNED)
    @variable = false
    @blob = false
    case
    when TYPES[base_type][:class] == IntegerType
      @length = TYPES[base_type][:length]
    when base_type == :CHAR
      @length = modifiers[0]
    when base_type == :VARCHAR
      @length = modifiers[0]
      @variable = true
    when base_type == :BLOB
      @blob = true
      @variable = true
    else
      raise "Data type '#{type_string}' is not supported"
    end
    @reader = TYPES[base_type][:class].new(self)
  end

  def unsigned?
    @unsigned
  end

  def variable?
    @variable
  end

  def blob?
    @blob
  end

  def name_suffix
    if [:CHAR, :VARCHAR].include?(base_type)
      "(#{length})"
    else
      ""
    end
  end

  def name
    "#{base_type}#{name_suffix}"
  end
end
