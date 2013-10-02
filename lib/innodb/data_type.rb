# -*- encoding : utf-8 -*-
class Innodb::DataType
  class GenericType
    attr_reader :name, :width, :data_type

    def initialize(data_type, modifiers, properties)
      @data_type = data_type
      @width = modifiers[0]
      @name = "%s(%d)" % [data_type.base_type.to_s, @width]
    end

    def variable?
      false
    end

    def blob?
      false
    end
  end

  class IntegerType < GenericType
    def initialize(data_type, modifiers, properties)
      @data_type = data_type
      @name = data_type.base_type.to_s
      @width = base_type_width_map[data_type.base_type]
      @unsigned = properties.include?(:UNSIGNED)
    end

    def base_type_width_map
      {
        :TINYINT    => 1,
        :SMALLINT   => 2,
        :MEDIUMINT  => 3,
        :INT        => 4,
        :INT6       => 6,
        :BIGINT     => 8,
      }
    end

    def value(data)
      nbits = @width * 8
      @unsigned ? get_uint(data, nbits) : get_int(data, nbits)
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

    def variable?
      true
    end
  end

  class BlobType < GenericType
    def variable?
      true
    end

    def blob?
      true
    end
  end

  # Maps base type to data type class.
  TYPES = {
    :TINYINT    => IntegerType,
    :SMALLINT   => IntegerType,
    :MEDIUMINT  => IntegerType,
    :INT        => IntegerType,
    :INT6       => IntegerType,
    :BIGINT     => IntegerType,
    :CHAR       => GenericType,
    :VARCHAR    => VariableStringType,
    :BLOB       => BlobType,
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
  attr_reader :reader
  def initialize(type_string, properties)
    @base_type, modifiers = self.class.parse_base_type_and_modifiers(type_string)
    raise "Data type '#{@base_type}' is not supported" unless TYPES.key?(@base_type)
    @reader = TYPES[base_type].new(self, modifiers, properties)
  end

  def variable?
    @reader.variable?
  end

  def blob?
    @reader.blob?
  end

  def width
    @reader.width
  end

  def name
    @reader.name
  end
end
