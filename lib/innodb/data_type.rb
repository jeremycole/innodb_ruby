# -*- encoding : utf-8 -*-
class Innodb::DataType

  class IntegerType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = base_type_width_map[base_type]
      @unsigned = properties.include?(:UNSIGNED)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
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

  # Fixed-length character type.
  class CharacterType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers[0]
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end
  end

  class VariableCharacterType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers[0]
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      # The SQL standard defines that VARCHAR fields should have end-spaces
      # stripped off.
      data.sub(/[ ]+$/, "")
    end
  end

  class BlobType
    attr_reader :name

    def initialize(base_type, modifiers, properties)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
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
    :CHAR       => CharacterType,
    :VARCHAR    => VariableCharacterType,
    :BLOB       => BlobType,
  }

  def self.make_name(base_type, modifiers, properties)
    name = base_type.to_s
    name << '(' + modifiers.join(',') + ')' if not modifiers.empty?
    name << " "
    name << properties.join(' ')
    name.strip
  end

  def self.new(base_type, modifiers, properties)
    raise "Data type '#{base_type}' is not supported" unless TYPES.key?(base_type)
    TYPES[base_type].new(base_type, modifiers, properties)
  end
end
