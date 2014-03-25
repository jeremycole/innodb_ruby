# -*- encoding : utf-8 -*-

require "spec_helper"

describe BufferCursor do
  before :all do
    @data = {
      :offset => {},
      :buffer => "",
    }

    # Bytes 0x00 through 0x0f at offset 0.
    @data[:offset][:bytes_00_0f] = @data[:buffer].size
    @data[:buffer] <<
      "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

    # Maximum-sized integers for each type.
    @data[:offset][:max_uint] = @data[:buffer].size
    @data[:buffer] << "\xff\xff\xff\xff\xff\xff\xff\xff"

    # A test string.
    @data[:offset][:alphabet] = @data[:buffer].size
    @data[:buffer] << "abcdefghijklmnopqrstuvwxyz"

    # InnoDB-compressed unsigned 32-bit integers.
    @data[:offset][:ic_uint32_00000000] = @data[:buffer].size
    @data[:buffer] << "\x00"

    @data[:offset][:ic_uint32_0000007f] = @data[:buffer].size
    @data[:buffer] << "\x7f"

    @data[:offset][:ic_uint32_00003fff] = @data[:buffer].size
    @data[:buffer] << "\xbf\xff"

    @data[:offset][:ic_uint32_001fffff] = @data[:buffer].size
    @data[:buffer] << "\xdf\xff\xff"

    @data[:offset][:ic_uint32_0fffffff] = @data[:buffer].size
    @data[:buffer] << "\xef\xff\xff\xff"

    @data[:offset][:ic_uint32_ffffffff] = @data[:buffer].size
    @data[:buffer] << "\xf0\xff\xff\xff\xff"

    # InnoDB-compressed unsigned 64-bit integers.
    @data[:offset][:ic_uint64_0000000000000000] = @data[:buffer].size
    @data[:buffer] << "\x00\x00\x00\x00\x00"

    @data[:offset][:ic_uint64_0000000100000001] = @data[:buffer].size
    @data[:buffer] << "\x01\x00\x00\x00\x01"

    @data[:offset][:ic_uint64_00000000ffffffff] = @data[:buffer].size
    @data[:buffer] << "\x00\xff\xff\xff\xff"

    @data[:offset][:ic_uint64_ffffffff00000000] = @data[:buffer].size
    @data[:buffer] << "\xf0\xff\xff\xff\xff\x00\x00\x00\x00"

    @data[:offset][:ic_uint64_0000ffff0000ffff] = @data[:buffer].size
    @data[:buffer] << "\xc0\xff\xff\x00\x00\xff\xff"

    @data[:offset][:ic_uint64_ffff0000ffff0000] = @data[:buffer].size
    @data[:buffer] << "\xf0\xff\xff\x00\x00\xff\xff\x00\x00"

    @data[:offset][:ic_uint64_ffffffffffffffff] = @data[:buffer].size
    @data[:buffer] << "\xf0\xff\xff\xff\xff\xff\xff\xff\xff"

    # InnoDB-"much compressed" unsigned 64-bit integers.
    @data[:offset][:imc_uint64_0000000000000000] = @data[:buffer].size
    @data[:buffer] << "\x00"

    @data[:offset][:imc_uint64_0000000100000001] = @data[:buffer].size
    @data[:buffer] << "\xff\x01\x01"

    @data[:offset][:imc_uint64_00000000ffffffff] = @data[:buffer].size
    @data[:buffer] << "\xf0\xff\xff\xff\xff"

    @data[:offset][:imc_uint64_ffffffff00000000] = @data[:buffer].size
    @data[:buffer] << "\xff\xf0\xff\xff\xff\xff\x00"

    @data[:offset][:imc_uint64_0000ffff0000ffff] = @data[:buffer].size
    @data[:buffer] << "\xff\xc0\xff\xff\xc0\xff\xff"

    @data[:offset][:imc_uint64_ffff0000ffff0000] = @data[:buffer].size
    @data[:buffer] << "\xff\xf0\xff\xff\x00\x00\xf0\xff\xff\x00\x00"

    @data[:offset][:imc_uint64_ffffffffffffffff] = @data[:buffer].size
    @data[:buffer] << "\xff\xf0\xff\xff\xff\xff\xf0\xff\xff\xff\xff"

    @buffer = @data[:buffer]
  end

  before :each do
    @cursor = BufferCursor.new(@buffer, 0)
  end

  describe "#new" do
    it "returns an BufferCursor" do
      @cursor.should be_an_instance_of BufferCursor
    end
  end

  describe "#position" do
    it "returns the position of the cursor" do
      @cursor.position.should eql 0
      @cursor.seek(1)
      @cursor.position.should eql 1
    end
  end

  describe "#seek" do
    it "moves the cursor to the provided position" do
      @cursor.position.should eql 0
      @cursor.seek(5)
      @cursor.position.should eql 5
      @cursor.seek(10)
      @cursor.position.should eql 10
    end
  end

  describe "#adjust" do
    it "adjusts the cursor forwards with positive values" do
      @cursor.position.should eql 0
      @cursor.adjust(5)
      @cursor.position.should eql 5
      @cursor.adjust(5)
      @cursor.position.should eql 10
    end

    it "adjusts the cursor backwards with negative values" do
      @cursor.position.should eql 0
      @cursor.adjust(5)
      @cursor.position.should eql 5
      @cursor.adjust(-5)
      @cursor.position.should eql 0
    end
  end

  describe "#read_and_advance" do
    it "reads the number of bytes specified" do
      @cursor.read_and_advance(1).should eql "\x00"
      @cursor.read_and_advance(2).should eql "\x01\x02"
      @cursor.read_and_advance(3).should eql "\x03\x04\x05"
    end
  end

  describe "#forward" do
    it "returns self" do
      @cursor.forward.should eql @cursor
    end

    it "sets the direction to forwards" do
      @cursor.forward
      @cursor.direction.should eql :forward
    end

    it "reads data forwards" do
      @cursor.seek(0)
      @cursor.forward
      @cursor.read_and_advance(1).should eql "\x00"
      @cursor.read_and_advance(1).should eql "\x01"
    end
  end

  describe "#backward" do
    it "returns self" do
      @cursor.backward.should eql @cursor
    end

    it "sets the direction to backward" do
      @cursor.backward
      @cursor.direction.should eql :backward
    end

    it "reads data backward" do
      @cursor.seek(5)
      @cursor.backward
      @cursor.read_and_advance(1).should eql "\x04"
      @cursor.read_and_advance(1).should eql "\x03"
    end
  end

  describe "#push and #pop" do
    it "returns self" do
      @cursor.push.should eql @cursor
      @cursor.pop.should eql @cursor
    end

    it "pushes and pops" do
      @cursor.push(10)
      @cursor.position.should eql 10
      @cursor.pop
      @cursor.position.should eql 0
    end
  end

  describe "#peek" do
    it "passes through the block return value" do
      @cursor.peek { true }.should eql true
      @cursor.peek { false }.should eql false
    end

    it "doesn't disturb the cursor position or direction on return" do
      @cursor.position.should eql 0
      @cursor.direction.should eql :forward
      @cursor.peek do
        @cursor.seek(10).backward
        @cursor.position.should eql 10
        @cursor.direction.should eql :backward
        @cursor.peek do
          @cursor.seek(20).forward
          @cursor.position.should eql 20
          @cursor.direction.should eql :forward
      end
        @cursor.position.should eql 10
        @cursor.direction.should eql :backward
    end
      @cursor.position.should eql 0
      @cursor.direction.should eql :forward
    end
  end

  describe "#get_bytes" do
    it "returns a raw byte string of the given length" do
      @cursor.get_bytes(4).should eql "\x00\x01\x02\x03"
    end

    it "returns a string uncorrupted" do
      @cursor.seek(@data[:offset][:alphabet])
      @cursor.get_bytes(4).should eql "abcd"
    end
  end

  describe "#get_hex" do
    it "returns a hex string of the given length" do
      @cursor.get_hex(4).should eql "00010203"
      @cursor.get_hex(4).should eql "04050607"
      @cursor.get_hex(4).should eql "08090a0b"
      @cursor.get_hex(4).should eql "0c0d0e0f"
    end
  end

  describe "#get_uint8" do
    it "reads 1 byte as uint8" do
      @cursor.get_uint8.should eql 0x00
      @cursor.get_uint8.should eql 0x01
      @cursor.get_uint8.should eql 0x02
      @cursor.get_uint8.should eql 0x03
      @cursor.seek(@data[:offset][:max_uint])
      @cursor.get_uint8.should eql 0xff
    end
  end

  describe "#get_uint16" do
    it "returns 2 bytes as uint16" do
      @cursor.get_uint16.should eql 0x0001
      @cursor.get_uint16.should eql 0x0203
      @cursor.get_uint16.should eql 0x0405
      @cursor.get_uint16.should eql 0x0607
      @cursor.seek(@data[:offset][:max_uint])
      @cursor.get_uint16.should eql 0xffff
    end
  end

  describe "#get_uint24" do
    it "returns 3 bytes as uint24" do
      @cursor.get_uint24.should eql 0x000102
      @cursor.get_uint24.should eql 0x030405
      @cursor.get_uint24.should eql 0x060708
      @cursor.get_uint24.should eql 0x090a0b
      @cursor.seek(@data[:offset][:max_uint])
      @cursor.get_uint24.should eql 0xffffff
    end
  end

  describe "#get_uint32" do
    it "returns 4 bytes as uint32" do
      @cursor.get_uint32.should eql 0x00010203
      @cursor.get_uint32.should eql 0x04050607
      @cursor.get_uint32.should eql 0x08090a0b
      @cursor.get_uint32.should eql 0x0c0d0e0f
      @cursor.seek(@data[:offset][:max_uint])
      @cursor.get_uint32.should eql 0xffffffff
    end
  end

  describe "#get_uint64" do
    it "returns 8 bytes as uint64" do
      @cursor.get_uint64.should eql 0x0001020304050607
      @cursor.get_uint64.should eql 0x08090a0b0c0d0e0f
      @cursor.seek(@data[:offset][:max_uint])
      @cursor.get_uint64.should eql 0xffffffffffffffff
    end
  end

  describe "#get_uint_by_size" do
    it "returns a uint8 for size 1" do
      @cursor.get_uint_by_size(1).should eql 0x00
    end

    it "returns a uint16 for size 2" do
      @cursor.get_uint_by_size(2).should eql 0x0001
    end

    it "returns a uint24 for size 3" do
      @cursor.get_uint_by_size(3).should eql 0x000102
    end

    it "returns a uint32 for size 4" do
      @cursor.get_uint_by_size(4).should eql 0x00010203
    end

    it "returns a uint64 for size 8" do
      @cursor.get_uint_by_size(8).should eql 0x0001020304050607
    end
  end

  describe "#get_ic_uint32" do
    it "reads a 1-byte zero value correctly" do
      @cursor.seek(@data[:offset][:ic_uint32_00000000])
      @cursor.get_ic_uint32.should eql 0
      @cursor.position.should eql @data[:offset][:ic_uint32_00000000] + 1
    end

    it "reads a 1-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:ic_uint32_0000007f])
      @cursor.get_ic_uint32.should eql 0x7f
      @cursor.position.should eql @data[:offset][:ic_uint32_0000007f] + 1
    end

    it "reads a 2-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:ic_uint32_00003fff])
      @cursor.get_ic_uint32.should eql 0x3fff
      @cursor.position.should eql @data[:offset][:ic_uint32_00003fff] + 2
    end

    it "reads a 3-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:ic_uint32_001fffff])
      @cursor.get_ic_uint32.should eql 0x1fffff
      @cursor.position.should eql @data[:offset][:ic_uint32_001fffff] + 3
    end

    it "reads a 4-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:ic_uint32_0fffffff])
      @cursor.get_ic_uint32.should eql 0x0fffffff
      @cursor.position.should eql @data[:offset][:ic_uint32_0fffffff] + 4
    end

    it "reads a 5-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:ic_uint32_ffffffff])
      @cursor.get_ic_uint32.should eql 0xffffffff
      @cursor.position.should eql @data[:offset][:ic_uint32_ffffffff] + 5
    end
  end

  describe "#get_ic_uint64" do
    it "reads a 5-byte zero value correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_0000000000000000])
      @cursor.get_ic_uint64.should eql 0
      @cursor.position.should eql @data[:offset][:ic_uint64_0000000000000000] + 5
    end

    it "reads a 5-byte interesting value 0x0000000100000001 correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_0000000100000001])
      @cursor.get_ic_uint64.should eql 0x0000000100000001
      @cursor.position.should eql @data[:offset][:ic_uint64_0000000100000001] + 5
    end

    it "reads a 5-byte interesting value 0x00000000ffffffff correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_00000000ffffffff])
      @cursor.get_ic_uint64.should eql 0x00000000ffffffff
      @cursor.position.should eql @data[:offset][:ic_uint64_00000000ffffffff] + 5
    end

    it "reads a 9-byte interesting value 0xffffffff00000000 correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_ffffffff00000000])
      @cursor.get_ic_uint64.should eql 0xffffffff00000000
      @cursor.position.should eql @data[:offset][:ic_uint64_ffffffff00000000] + 9
    end

    it "reads a 7-byte interesting value 0x0000ffff0000ffff correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_0000ffff0000ffff])
      @cursor.get_ic_uint64.should eql 0x0000ffff0000ffff
      @cursor.position.should eql @data[:offset][:ic_uint64_0000ffff0000ffff] + 7
    end

    it "reads a 9-byte interesting value 0xffff0000ffff0000 correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_ffff0000ffff0000])
      @cursor.get_ic_uint64.should eql 0xffff0000ffff0000
      @cursor.position.should eql @data[:offset][:ic_uint64_ffff0000ffff0000] + 9
    end

    it "reads a 9-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:ic_uint64_ffffffffffffffff])
      @cursor.get_ic_uint64.should eql 0xffffffffffffffff
      @cursor.position.should eql @data[:offset][:ic_uint64_ffffffffffffffff] + 9
    end
  end

  describe "#get_imc_uint64" do
    it "reads a 1-byte zero value correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_0000000000000000])
      @cursor.get_imc_uint64.should eql 0
      @cursor.position.should eql @data[:offset][:imc_uint64_0000000000000000] + 1
    end

    it "reads a 3-byte interesting value 0x0000000100000001 correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_0000000100000001])
      @cursor.get_imc_uint64.should eql 0x0000000100000001
      @cursor.position.should eql @data[:offset][:imc_uint64_0000000100000001] + 3
    end

    it "reads a 5-byte interesting value 0x00000000ffffffff correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_00000000ffffffff])
      @cursor.get_imc_uint64.should eql 0x00000000ffffffff
      @cursor.position.should eql @data[:offset][:imc_uint64_00000000ffffffff] + 5
    end

    it "reads a 7-byte interesting value 0xffffffff00000000 correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_ffffffff00000000])
      @cursor.get_imc_uint64.should eql 0xffffffff00000000
      @cursor.position.should eql @data[:offset][:imc_uint64_ffffffff00000000] + 7
    end

    it "reads a 7-byte interesting value 0x0000ffff0000ffff correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_0000ffff0000ffff])
      @cursor.get_imc_uint64.should eql 0x0000ffff0000ffff
      @cursor.position.should eql @data[:offset][:imc_uint64_0000ffff0000ffff] + 7
    end

    it "reads a 11-byte interesting value 0xffff0000ffff0000 correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_ffff0000ffff0000])
      @cursor.get_imc_uint64.should eql 0xffff0000ffff0000
      @cursor.position.should eql @data[:offset][:imc_uint64_ffff0000ffff0000] + 11
    end

    it "reads a 11-byte maximal value correctly" do
      @cursor.seek(@data[:offset][:imc_uint64_ffffffffffffffff])
      @cursor.get_imc_uint64.should eql 0xffffffffffffffff
      @cursor.position.should eql @data[:offset][:imc_uint64_ffffffffffffffff] + 11
    end
  end

  describe "#get_bit_array" do
    it "returns an array of bits" do
      @cursor.get_bit_array(64).uniq.sort.should eql [0, 1]
    end

    it "returns the right bits" do
      @cursor.get_bit_array(8).should eql [0, 0, 0, 0, 0, 0, 0, 0]
      @cursor.get_bit_array(8).should eql [0, 0, 0, 0, 0, 0, 0, 1]
      @cursor.seek(@data[:offset][:max_uint])
      @cursor.get_bit_array(8).should eql [1, 1, 1, 1, 1, 1, 1, 1]
    end

    it "can handle large bit arrays" do
      @cursor.get_bit_array(64).size.should eql 64
    end
  end

  describe "#trace!" do
    it "enables tracing globally" do
      BufferCursor.trace!

      trace_string = ""
      trace_output = StringIO.new(trace_string, "w")

      c1 = BufferCursor.new(@buffer, 0)
      c1.trace_to(trace_output)
      c1.get_bytes(4).should eql "\x00\x01\x02\x03"

      trace_string.should match(/000000 → 00010203/)

      c2 = BufferCursor.new(@buffer, 0)
      c2.trace_to(trace_output)
      c2.seek(4).get_bytes(4).should eql "\x04\x05\x06\x07"

      trace_string.should match(/000004 → 04050607/)

      BufferCursor.trace!(false)
    end
  end

  describe "#trace" do
    it "enables tracing per instance" do
      trace_string = ""
      trace_output = StringIO.new(trace_string, "w")

      c1 = BufferCursor.new(@buffer, 0)
      c1.trace
      c1.trace_to(trace_output)
      c1.get_bytes(4).should eql "\x00\x01\x02\x03"

      trace_string.should match(/000000 → 00010203/)

      c2 = BufferCursor.new(@buffer, 0)
      c2.trace_to(trace_output)
      c2.seek(4).get_bytes(4).should eql "\x04\x05\x06\x07"

      trace_string.should_not match(/000004 → 04050607/)
    end
  end
end
