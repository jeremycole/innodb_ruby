class Innodb::Field
  attr_reader :position, :nullable, :fixed_len, :variable_len

  def initialize(position, type, *properties)
    @position = position
    @type, @fixed_len, @variable_len = parse_data_type(type.to_s)
    @nullable = (not properties.include?(:NOT_NULL))
    @unsigned = properties.include?(:UNSIGNED)
  end

  # Parse the data type description string of a field.
  def parse_data_type(data_type)
    case data_type
    when /^(tinyint|smallint|mediumint|int|bigint)$/i
      type = data_type.upcase.to_sym
      fixed_len = fixed_len_map[type]
      [type, fixed_len, 0]
    when /varchar\((\d+)\)$/i
      [:VARCHAR, 0, $1.to_i]
    else
      raise "Data type '#{data_type}' is not supported"
    end
  end

  # Maps data type to fixed storage length.
  def fixed_len_map
    {
      :TINYINT    => 1,
      :SMALLINT   => 2,
      :MEDIUMINT  => 3,
      :INT        => 4,
      :BIGINT     => 8,
    }
  end

  # Return whether this field is NULL.
  def null?(record)
    case record[:format]
    when :compact
      header = record[:header]
      header[:null_bitmap][@position]
    end
  end

  # Return the length of this variable-length field.
  def get_variable_len(record)
    case record[:format]
    when :compact
      header = record[:header]
      header[:variable_length][@position]
    end
  end

  # Read an InnoDB encoded data field.
  def read(record, cursor)
    return :NULL if @nullable and null?(record)

    case @type
    when :TINYINT, :SMALLINT, :MEDIUMINT, :INT, :BIGINT
      symbol = @unsigned ? :get_uint_by_size : :get_i_sint_by_size
      cursor.send(symbol, @fixed_len)
    when :VARCHAR
      '\'' + cursor.get_bytes(get_variable_len(record)) + '\''
    end
  end
end
