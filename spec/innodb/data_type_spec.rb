# -*- encoding : utf-8 -*-

require "spec_helper"
require "stringio"

describe Innodb::DataType do

  it "makes proper data type names" do
    Innodb::DataType.make_name("BIGINT", [], [:UNSIGNED]).should eql "BIGINT UNSIGNED"
    Innodb::DataType.make_name("SMALLINT", [], []).should eql "SMALLINT"
    Innodb::DataType.make_name("VARCHAR", [32], []).should eql "VARCHAR(32)"
    Innodb::DataType.make_name("CHAR", [16], []).should eql "CHAR(16)"
    Innodb::DataType.make_name("CHAR", [], []).should eql "CHAR"
    Innodb::DataType.make_name("VARBINARY", [48], []).should eql "VARBINARY(48)"
    Innodb::DataType.make_name("BINARY", [64], []).should eql "BINARY(64)"
    Innodb::DataType.make_name("BINARY", [], []).should eql "BINARY"
  end

  describe Innodb::DataType::CharacterType do
    it "handles optional width" do
      Innodb::DataType.new(:CHAR, [], []).width.should eql 1
      Innodb::DataType.new(:CHAR, [16], []).width.should eql 16
    end
  end

  describe Innodb::DataType::VariableCharacterType do
    it "throws an error on invalid modifiers" do
      expect { Innodb::DataType.new(:VARCHAR, [], []) }.
        to raise_error "Invalid width specification"
      expect { Innodb::DataType.new(:VARCHAR, [1,1], []) }.
        to raise_error "Invalid width specification"
    end
  end

  describe Innodb::DataType::BinaryType do
    it "handles optional width" do
      Innodb::DataType.new(:BINARY, [], []).width.should eql 1
      Innodb::DataType.new(:BINARY, [16], []).width.should eql 16
    end
  end

  describe Innodb::DataType::VariableBinaryType do
    it "throws an error on invalid modifiers" do
      expect { Innodb::DataType.new(:VARBINARY, [], []) }.
        to raise_error "Invalid width specification"
      expect { Innodb::DataType.new(:VARBINARY, [1,1], []) }.
        to raise_error "Invalid width specification"
    end
  end

  describe Innodb::DataType::IntegerType do
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

      # InnoDB-munged signed positive integers.
      @data[:offset][:innodb_sint_pos] = @data[:buffer].size
      @data[:buffer] <<
        "\x80\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

      # InnoDB-munged signed negative integers.
      @data[:offset][:innodb_sint_neg] = @data[:buffer].size
      @data[:buffer] <<
        "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

      @buffer = StringIO.new(@data[:buffer])
    end

    before(:each) do
      @buffer.seek(@data[:offset][:bytes_00_0f])
    end

    it "returns a TINYINT value correctly" do
      data_type = Innodb::DataType.new(:TINYINT, [], [])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(1)).should eql 0x00
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(1)).should eql -128
    end

    it "returns a TINYINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.new(:TINYINT, [], [:UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      data_type.value(@buffer.read(1)).should eql 0x00
      data_type.value(@buffer.read(1)).should eql 0x01
      data_type.value(@buffer.read(1)).should eql 0x02
      data_type.value(@buffer.read(1)).should eql 0x03
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(1)).should eql 0xff
    end

    it "returns a SMALLINT value correctly" do
      data_type = Innodb::DataType.new(:SMALLINT, [], [])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(2)).should eql 0x0001
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(2)).should eql -32767
    end

    it "returns a SMALLINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.new(:SMALLINT, [], [:UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      data_type.value(@buffer.read(2)).should eql 0x0001
      data_type.value(@buffer.read(2)).should eql 0x0203
      data_type.value(@buffer.read(2)).should eql 0x0405
      data_type.value(@buffer.read(2)).should eql 0x0607
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(2)).should eql 0xffff
    end

    it "returns a MEDIUMINT value correctly" do
      data_type = Innodb::DataType.new(:MEDIUMINT, [], [])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(3)).should eql 0x000102
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(3)).should eql -8388350
    end

    it "returns a MEDIUMINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.new(:MEDIUMINT, [], [:UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      data_type.value(@buffer.read(3)).should eql 0x000102
      data_type.value(@buffer.read(3)).should eql 0x030405
      data_type.value(@buffer.read(3)).should eql 0x060708
      data_type.value(@buffer.read(3)).should eql 0x090a0b
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(3)).should eql 0xffffff
    end

    it "returns an INT value correctly" do
      data_type = Innodb::DataType.new(:INT, [], [])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(4)).should eql 0x00010203
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(4)).should eql -2147417597
    end

    it "returns an INT UNSIGNED value correctly" do
      data_type = Innodb::DataType.new(:INT, [], [:UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::IntegerType
      data_type.value(@buffer.read(4)).should eql 0x00010203
      data_type.value(@buffer.read(4)).should eql 0x04050607
      data_type.value(@buffer.read(4)).should eql 0x08090a0b
      data_type.value(@buffer.read(4)).should eql 0x0c0d0e0f
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(4)).should eql 0xffffffff
    end

    it "returns a BIGINT value correctly" do
      data_type = Innodb::DataType.new(:BIGINT, [], [])
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(8)).should eql 0x0001020304050607
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(8)).should eql -9223088349902469625
    end

    it "returns a BIGINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.new(:BIGINT, [], [:UNSIGNED])
      data_type.value(@buffer.read(8)).should eql 0x0001020304050607
      data_type.value(@buffer.read(8)).should eql 0x08090a0b0c0d0e0f
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(8)).should eql 0xffffffffffffffff
    end
  end
end
