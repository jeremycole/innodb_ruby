# frozen_string_literal: true

require "spec_helper"
require "stringio"

describe Innodb::DataType do
  describe Innodb::DataType::Character do
    it "handles optional length" do
      Innodb::DataType.parse("CHAR", []).length.should eql 1
      Innodb::DataType.parse("CHAR(16)", []).length.should eql 16
    end

    it "throws an error on invalid modifiers" do
      expect { Innodb::DataType.parse("VARCHAR", []) }.to raise_error Innodb::DataType::InvalidSpecificationError
      expect { Innodb::DataType.parse("VARCHAR(1,1)", []) }.to raise_error Innodb::DataType::InvalidSpecificationError
    end

    it "handles optional length" do
      Innodb::DataType.parse("BINARY", []).length.should eql 1
      Innodb::DataType.parse("BINARY(16)", []).length.should eql 16
    end

    it "throws an error on invalid modifiers" do
      expect { Innodb::DataType.parse("VARBINARY", []) }.to raise_error Innodb::DataType::InvalidSpecificationError
      expect { Innodb::DataType.parse("VARBINARY(1,1)", []) }.to raise_error Innodb::DataType::InvalidSpecificationError
    end
  end

  describe Innodb::DataType::Integer do
    before :all do
      @data = {
        offset: {},
        buffer: "".dup,
      }

      # Bytes 0x00 through 0x0f at offset 0.
      @data[:offset][:bytes_00_0f] = @data[:buffer].size
      @data[:buffer] << "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

      # Maximum-sized integers for each type.
      @data[:offset][:max_uint] = @data[:buffer].size
      @data[:buffer] << "\xff\xff\xff\xff\xff\xff\xff\xff"

      # InnoDB-munged signed positive integers.
      @data[:offset][:innodb_sint_pos] = @data[:buffer].size
      @data[:buffer] << "\x80\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

      # InnoDB-munged signed negative integers.
      @data[:offset][:innodb_sint_neg] = @data[:buffer].size
      @data[:buffer] << "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"

      @buffer = StringIO.new(@data[:buffer])
    end

    before(:each) do
      @buffer.seek(@data[:offset][:bytes_00_0f])
    end

    it "returns a TINYINT value correctly" do
      data_type = Innodb::DataType.parse("TINYINT", [])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(1)).should eql 0x00
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(1)).should eql -128
    end

    it "returns a TINYINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.parse("TINYINT", %i[UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      data_type.value(@buffer.read(1)).should eql 0x00
      data_type.value(@buffer.read(1)).should eql 0x01
      data_type.value(@buffer.read(1)).should eql 0x02
      data_type.value(@buffer.read(1)).should eql 0x03
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(1)).should eql 0xff
    end

    it "returns a SMALLINT value correctly" do
      data_type = Innodb::DataType.parse("SMALLINT", [])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(2)).should eql 0x0001
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(2)).should eql -32_767
    end

    it "returns a SMALLINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.parse("SMALLINT", %i[UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      data_type.value(@buffer.read(2)).should eql 0x0001
      data_type.value(@buffer.read(2)).should eql 0x0203
      data_type.value(@buffer.read(2)).should eql 0x0405
      data_type.value(@buffer.read(2)).should eql 0x0607
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(2)).should eql 0xffff
    end

    it "returns a MEDIUMINT value correctly" do
      data_type = Innodb::DataType.parse("MEDIUMINT", [])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(3)).should eql 0x000102
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(3)).should eql -8_388_350
    end

    it "returns a MEDIUMINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.parse("MEDIUMINT", %i[UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      data_type.value(@buffer.read(3)).should eql 0x000102
      data_type.value(@buffer.read(3)).should eql 0x030405
      data_type.value(@buffer.read(3)).should eql 0x060708
      data_type.value(@buffer.read(3)).should eql 0x090a0b
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(3)).should eql 0xffffff
    end

    it "returns an INT value correctly" do
      data_type = Innodb::DataType.parse("INT", [])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(4)).should eql 0x00010203
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(4)).should eql -2_147_417_597
    end

    it "returns an INT UNSIGNED value correctly" do
      data_type = Innodb::DataType.parse("INT", %i[UNSIGNED])
      data_type.should be_an_instance_of Innodb::DataType::Integer
      data_type.value(@buffer.read(4)).should eql 0x00010203
      data_type.value(@buffer.read(4)).should eql 0x04050607
      data_type.value(@buffer.read(4)).should eql 0x08090a0b
      data_type.value(@buffer.read(4)).should eql 0x0c0d0e0f
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(4)).should eql 0xffffffff
    end

    it "returns a BIGINT value correctly" do
      data_type = Innodb::DataType.parse("BIGINT", [])
      @buffer.seek(@data[:offset][:innodb_sint_pos])
      data_type.value(@buffer.read(8)).should eql 0x0001020304050607
      @buffer.seek(@data[:offset][:innodb_sint_neg])
      data_type.value(@buffer.read(8)).should eql -9_223_088_349_902_469_625
    end

    it "returns a BIGINT UNSIGNED value correctly" do
      data_type = Innodb::DataType.parse("BIGINT", %i[UNSIGNED])
      data_type.value(@buffer.read(8)).should eql 0x0001020304050607
      data_type.value(@buffer.read(8)).should eql 0x08090a0b0c0d0e0f
      @buffer.seek(@data[:offset][:max_uint])
      data_type.value(@buffer.read(8)).should eql 0xffffffffffffffff
    end
  end
end
