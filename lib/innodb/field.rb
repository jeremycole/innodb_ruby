# A single field in an InnoDB record (within an INDEX page). This class
# provides essential information to parse records: the length of the fixed
# width portion of the field
class Innodb::Field
  attr_reader :position, :nullable, :fixed_length, :variable_length

  def initialize(position, type, *properties)
    @position = position
    @type, @fixed_length, @variable_length = parse_data_type(type.to_s)
    @nullable = (not properties.include?(:NOT_NULL))
    @unsigned = properties.include?(:UNSIGNED)
  end

  # Parse the data type description string of a field.
  def parse_data_type(data_type)
    case data_type
    when /^(tinyint|smallint|mediumint|int|int6|bigint)$/i
      type = data_type.upcase.to_sym
      fixed_length = fixed_length_map[type]
      [type, fixed_length, 0]
    when /^varchar\((\d+)\)$/i
      [:VARCHAR, 0, $1.to_i]
    when /^char\((\d+)\)$/i
      [:CHAR, $1.to_i, 0]
    else
      raise "Data type '#{data_type}' is not supported"
    end
  end

  # Maps data type to fixed storage length.
  def fixed_length_map
    {
      :TINYINT    => 1,
      :SMALLINT   => 2,
      :MEDIUMINT  => 3,
      :INT        => 4,
      :INT6       => 6,
      :BIGINT     => 8,
    }
  end

  # Return whether this field is NULL.
  def null?(record)
    record[:header][:field_nulls][@position]
  end

  # Return the length of this variable-length field.
  def length(record)
    record[:header][:field_lengths][@position]
  end

  # Read an InnoDB encoded data field.
  def read(record, cursor)
    return :NULL if @nullable and null?(record)

    case @type
    when :TINYINT, :SMALLINT, :MEDIUMINT, :INT, :INT6, :BIGINT
      symbol = @unsigned ? :get_uint_by_size : :get_i_sint_by_size
      cursor.name("#{@type}") { cursor.send(symbol, @fixed_length) }
    when :VARCHAR
      len = length(record)
      cursor.name("VARCHAR(#{len})") { cursor.get_bytes(len) }
    when :CHAR
      # Fixed-width character fields will be space-padded up to their length,
      # so SQL defines that trailing spaces should be removed.
      cursor.name("CHAR(#{fixed_length})") {
        cursor.get_bytes(fixed_length).sub(/[ ]+$/, "")
      }
    end
  end
end
