require 'spec_helper'

describe Innodb::Space do
  before :all do
    @space = Innodb::Space.new("spec/data/ibdata1")
  end

  describe "#new" do
    it "defines a class" do
      Innodb::Space.should be_an_instance_of Class
    end
  
    it "returns an Innodb::Space" do
      @space.should be_an_instance_of Innodb::Space
    end
  end

  describe "#read_at_offset" do
    it "can read bytes from the file" do
      # This will read the page number from page 0, which should be 0.
      @space.read_at_offset(4, 4).should eql "\x00\x00\x00\x00"
      # This will read the page number from page 1, which should be 1.
      @space.read_at_offset(16384+4, 4).should eql "\x00\x00\x00\x01"
    end
  end

  describe "#page_size" do
    it "finds a 16 KiB page size" do
      @space.page_size.should eql 16384
    end
  end

  describe "#pages" do
    it "returns 1152 pages" do
      @space.pages.should eql 1152
    end
  end

  describe "#size" do
    it "returns 18874368 bytes" do
      @space.size.should eql 18874368
    end
  end

  describe "#pages_per_extent" do
    it "returns 64 pages per extent" do
      @space.pages_per_extent.should eql 64
    end
  end

  describe "#page_data" do
    it "returns 16 KiB of data" do
      @space.page_data(0).size.should eql 16384
    end
  end

  describe "#page" do
    it "reads and delegates pages correctly" do
      @space.page(0).should be_an_instance_of Innodb::Page::FspHdrXdes
      @space.page(1).should be_an_instance_of Innodb::Page
      @space.page(2).should be_an_instance_of Innodb::Page::Inode
      @space.page(3).should be_an_instance_of Innodb::Page
      @space.page(4).should be_an_instance_of Innodb::Page::Index
      @space.page(5).should be_an_instance_of Innodb::Page::TrxSys
      @space.page(6).should be_an_instance_of Innodb::Page
      @space.page(7).should be_an_instance_of Innodb::Page
    end
  end

  describe "#fsp" do
    it "is a Hash" do
      @space.fsp.should be_an_instance_of Hash
    end
  end
end
