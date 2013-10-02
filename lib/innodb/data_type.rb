# -*- encoding : utf-8 -*-
class Innodb::DataType
  class GenericType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers[0]
      @name = "%s(%d)" % [base_type.to_s, @width]
    end

    def variable?
      false
    end

    def blob?
      false
    end
  end

  class IntegerType < GenericType
    def initialize(base_type, modifiers, properties)
      @name = base_type.to_s
      @width = base_type_width_map[base_type]
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

  def self.new(base_type, modifiers, properties)
    raise "Data type '#{base_type}' is not supported" unless TYPES.key?(base_type)
    TYPES[base_type].new(base_type, modifiers, properties)
  end
end
