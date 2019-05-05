# -*- encoding : utf-8 -*-

require "stringio"
require "bigdecimal"
require "date"

class Innodb::DataType

  # MySQL's Bit-Value Type (BIT).
  class BitType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      nbits = modifiers.fetch(0, 1)
      raise "Unsupported width for BIT type." unless nbits >= 0 and nbits <= 64
      @width = (nbits + 7) / 8
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      "0b%b" % BinData::const_get("Uint%dbe" % (@width * 8)).read(data)
    end
  end

  class IntegerType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = base_type_width_map[base_type]
      @unsigned = properties.include?(:UNSIGNED)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def base_type_width_map
      {
        :BOOL       => 1,
        :BOOLEAN    => 1,
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

  class FloatType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 4
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    # Read a little-endian single-precision floating-point number.
    def value(data)
      BinData::FloatLe.read(data)
    end
  end

  class DoubleType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 8
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    # Read a little-endian double-precision floating-point number.
    def value(data)
      BinData::DoubleLe.read(data)
    end
  end

  # MySQL's Fixed-Point Type (DECIMAL), stored in InnoDB as a binary string.
  class DecimalType
    attr_reader :name, :width

    # The value is stored as a sequence of signed big-endian integers, each
    # representing up to 9 digits of the integral and fractional parts. The
    # first integer of the integral part and/or the last integer of the
    # fractional part might be compressed (or packed) and are of variable
    # length. The remaining integers (if any) are uncompressed and 32 bits
    # wide.
    MAX_DIGITS_PER_INTEGER = 9
    BYTES_PER_DIGIT = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]

    def initialize(base_type, modifiers, properties)
      precision, scale = sanity_check(modifiers)
      integral = precision - scale
      @uncomp_integral = integral / MAX_DIGITS_PER_INTEGER
      @uncomp_fractional = scale / MAX_DIGITS_PER_INTEGER
      @comp_integral = integral - (@uncomp_integral * MAX_DIGITS_PER_INTEGER)
      @comp_fractional = scale - (@uncomp_fractional * MAX_DIGITS_PER_INTEGER)
      @width = @uncomp_integral * 4 + BYTES_PER_DIGIT[@comp_integral] +
               @comp_fractional * 4 + BYTES_PER_DIGIT[@comp_fractional]
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      # Strings representing the integral and fractional parts.
      intg, frac = "", ""

      stream = StringIO.new(data)
      mask = sign_mask(stream)

      intg << get_digits(stream, mask, @comp_integral)

      (1 .. @uncomp_integral).each do
        intg << get_digits(stream, mask, MAX_DIGITS_PER_INTEGER)
      end

      (1 .. @uncomp_fractional).each do
        frac << get_digits(stream, mask, MAX_DIGITS_PER_INTEGER)
      end

      frac << get_digits(stream, mask, @comp_fractional)
      frac = "0" if frac.empty?

      # Convert to something resembling a string representation.
      str = mask.to_s.chop + intg + '.' + frac

      BigDecimal(str).to_s('F')
    end

    private

    # Ensure width specification (if any) is compliant.
    def sanity_check(modifiers)
      raise "Invalid width specification" unless modifiers.size <= 2
      precision = modifiers.fetch(0, 10)
      raise "Unsupported precision for DECIMAL type" unless
        precision >= 1 and precision <= 65
      scale = modifiers.fetch(1, 0)
      raise "Unsupported scale for DECIMAL type" unless
        scale >= 0 and scale <= 30 and scale <= precision
      [precision, scale]
    end

    # The sign is encoded in the high bit of the first byte/digit. The byte
    # might be part of a larger integer, so apply the bit-flipper and push
    # back the byte into the stream.
    def sign_mask(stream)
      byte = BinData::Uint8.read(stream)
      sign = byte & 0x80
      byte.assign(byte ^ 0x80)
      stream.rewind
      byte.write(stream)
      stream.rewind
      (sign == 0) ? -1 : 0
    end

    # Return a string representing an integer with a specific number of digits.
    def get_digits(stream, mask, digits)
      nbits = BYTES_PER_DIGIT[digits] * 8
      return "" unless nbits > 0
      value = (BinData::const_get("Int%dbe" % nbits).read(stream) ^ mask)
      # Preserve leading zeros.
      ("%0" + digits.to_s + "d") % value
    end
  end

  # Fixed-length character type.
  class CharacterType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers.fetch(0, 1)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      # The SQL standard defines that CHAR fields should have end-spaces
      # stripped off.
      data.sub(/[ ]+$/, "")
    end
  end

  class VariableCharacterType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers[0]
      raise "Invalid width specification" unless modifiers.size == 1
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      # The SQL standard defines that VARCHAR fields should have end-spaces
      # stripped off.
      data.sub(/[ ]+$/, "")
    end
  end

  # Fixed-length binary type.
  class BinaryType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers.fetch(0, 1)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end
  end

  class VariableBinaryType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = modifiers[0]
      raise "Invalid width specification" unless modifiers.size == 1
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end
  end

  class BlobType
    attr_reader :name

    def initialize(base_type, modifiers, properties)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end
  end

  class YearType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 1
      @display_width = modifiers.fetch(0, 4)
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      year = BinData::Uint8.read(data)
      return (year % 100).to_s if @display_width != 4
      return (year + 1900).to_s if year != 0
      "0000"
    end
  end

  class TimeType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 3
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      time = BinData::Int24be.read(data) ^ (-1 << 23)
      sign = "-" if time < 0
      time = time.abs
      "%s%02d:%02d:%02d" % [sign, time / 10000, (time / 100) % 100, time % 100]
    end
  end

  class DateType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 3
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      date = BinData::Int24be.read(data) ^ (-1 << 23)
      day = date & 0x1f
      month = (date >> 5) & 0xf
      year = date >> 9
      "%04d-%02d-%02d" % [year, month, day]
    end
  end

  class DatetimeType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 8
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def value(data)
      datetime = BinData::Int64be.read(data) ^ (-1 << 63)
      date = datetime / 1000000
      year, month, day = [date / 10000, (date / 100) % 100, date % 100]
      time = datetime - (date * 1000000)
      hour, min, sec = [time / 10000, (time / 100) % 100, time % 100]
      "%04d-%02d-%02d %02d:%02d:%02d" % [year, month, day, hour, min, sec]
    end
  end

  class TimestampType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 4
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    # Returns the UTC timestamp as a value in 'YYYY-MM-DD HH:MM:SS' format.
    def value(data)
      timestamp = BinData::Uint32be.read(data)
      return "0000-00-00 00:00:00" if timestamp.zero?
      DateTime.strptime(timestamp.to_s, '%s').strftime "%Y-%m-%d %H:%M:%S"
    end
  end

  #
  # Data types for InnoDB system columns.
  #

  # Transaction ID.
  class TransactionIdType
    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 6
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def read(c)
      c.name("transaction_id") { c.get_uint48 }
    end
  end

  # Rollback data pointer.
  class RollPointerType
    extend ReadBitsAtOffset

    attr_reader :name, :width

    def initialize(base_type, modifiers, properties)
      @width = 7
      @name = Innodb::DataType.make_name(base_type, modifiers, properties)
    end

    def self.parse_roll_pointer(roll_ptr)
      {
        :is_insert  => read_bits_at_offset(roll_ptr, 1, 55) == 1,
        :rseg_id    => read_bits_at_offset(roll_ptr, 7, 48),
        :undo_log   => {
          :page   => read_bits_at_offset(roll_ptr, 32, 16),
          :offset => read_bits_at_offset(roll_ptr, 16, 0),
        }
      }
    end

    def value(data)
      roll_ptr = BinData::Uint56be.read(data)
      self.class.parse_roll_pointer(roll_ptr)
    end

  end

  # Maps base type to data type class.
  TYPES = {
    :BIT        => BitType,
    :BOOL       => IntegerType,
    :BOOLEAN    => IntegerType,
    :TINYINT    => IntegerType,
    :SMALLINT   => IntegerType,
    :MEDIUMINT  => IntegerType,
    :INT        => IntegerType,
    :INT6       => IntegerType,
    :BIGINT     => IntegerType,
    :FLOAT      => FloatType,
    :DOUBLE     => DoubleType,
    :DECIMAL    => DecimalType,
    :NUMERIC    => DecimalType,
    :CHAR       => CharacterType,
    :VARCHAR    => VariableCharacterType,
    :BINARY     => BinaryType,
    :VARBINARY  => VariableBinaryType,
    :TINYBLOB   => BlobType,
    :BLOB       => BlobType,
    :MEDIUMBLOB => BlobType,
    :LONGBLOB   => BlobType,
    :TINYTEXT   => BlobType,
    :TEXT       => BlobType,
    :MEDIUMTEXT => BlobType,
    :LONGTEXT   => BlobType,
    :YEAR       => YearType,
    :TIME       => TimeType,
    :DATE       => DateType,
    :DATETIME   => DatetimeType,
    :TIMESTAMP  => TimestampType,
    :TRX_ID     => TransactionIdType,
    :ROLL_PTR   => RollPointerType,
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
