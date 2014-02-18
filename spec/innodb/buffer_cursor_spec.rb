# -*- encoding : utf-8 -*-

require "spec_helper"

describe BufferCursor do
  before :all do
    @data = {
      :offset => {},
      :buffer => "",
    }

    # InnoDB-compressed unsigned 32-bit integers.
    @data[:offset][:innodb_comp_uint32_1] = @data[:buffer].size
    @data[:buffer] << "\x7f"

    @data[:offset][:innodb_comp_uint32_2] = @data[:buffer].size
    @data[:buffer] << "\xbf\xff"

    @data[:offset][:innodb_comp_uint32_3] = @data[:buffer].size
    @data[:buffer] << "\xdf\xff\xff"

    @data[:offset][:innodb_comp_uint32_4] = @data[:buffer].size
    @data[:buffer] << "\xef\xff\xff\xff"

    @data[:offset][:innodb_comp_uint32_5] = @data[:buffer].size
    @data[:buffer] << "\xf0\xff\xff\xff\xff"

    @buffer = @data[:buffer]
  end

  before :each do
    @cursor = BufferCursor.new(@buffer, 0)
  end

  describe "#get_ic_uint32" do
    it "reads an InnoDB-compressed 1-byte uint correctly" do
      @cursor.seek(@data[:offset][:innodb_comp_uint32_1])
      @cursor.get_ic_uint32.should eql 0xff ^ 0x80
      @cursor.position.should eql @data[:offset][:innodb_comp_uint32_1] + 1
    end

    it "reads an InnoDB-compressed 2-byte uint correctly" do
      @cursor.seek(@data[:offset][:innodb_comp_uint32_2])
      @cursor.get_ic_uint32.should eql 0xffff ^ 0xc000
      @cursor.position.should eql @data[:offset][:innodb_comp_uint32_2] + 2
    end

    it "reads an InnoDB-compressed 3-byte uint correctly" do
      @cursor.seek(@data[:offset][:innodb_comp_uint32_3])
      @cursor.get_ic_uint32.should eql 0xffffff ^ 0xe00000
      @cursor.position.should eql @data[:offset][:innodb_comp_uint32_3] + 3
    end

    it "reads an InnoDB-compressed 4-byte uint correctly" do
      @cursor.seek(@data[:offset][:innodb_comp_uint32_4])
      @cursor.get_ic_uint32.should eql 0xffffffff ^ 0xf0000000
      @cursor.position.should eql @data[:offset][:innodb_comp_uint32_4] + 4
    end

    it "reads an InnoDB-compressed 5-byte uint correctly" do
      @cursor.seek(@data[:offset][:innodb_comp_uint32_5])
      @cursor.get_ic_uint32.should eql 0xffffffff
      @cursor.position.should eql @data[:offset][:innodb_comp_uint32_5] + 5
    end
  end
end
