# -*- encoding : utf-8 -*-

require 'spec_helper'

describe Innodb::Space do
  before :all do
    @space = Innodb::Space.new("spec/data/ibdata1")
    @space_ibd = Innodb::Space.new("spec/data/t_empty.ibd")
  end

  describe "DEFAULT_PAGE_SIZE" do
    it "is a Fixnum" do
      Innodb::Space::DEFAULT_PAGE_SIZE.should be_an_instance_of Fixnum
    end
  end

  describe "SYSTEM_SPACE_PAGE_MAP" do
    it "is a Hash" do
      Innodb::Space::SYSTEM_SPACE_PAGE_MAP.should be_an_instance_of Hash
    end
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
      @space.page(1).should be_an_instance_of Innodb::Page::IbufBitmap
      @space.page(2).should be_an_instance_of Innodb::Page::Inode
      @space.page(3).should be_an_instance_of Innodb::Page::SysIbufHeader
      @space.page(4).should be_an_instance_of Innodb::Page::Index::Uncompressed
      @space.page(5).should be_an_instance_of Innodb::Page::TrxSys
      @space.page(6).should be_an_instance_of Innodb::Page::SysRsegHeader
      @space.page(7).should be_an_instance_of Innodb::Page::SysDataDictionaryHeader
    end
  end

  describe "#fsp" do
    it "is a Hash" do
      @space.fsp.should be_an_instance_of Hash
    end
  end

  describe "#system_space?" do
    it "can identify a system space" do
      @space.system_space?.should eql true
    end

    it "can identify a non-system space" do
      @space_ibd.system_space?.should eql false
    end
  end

  describe "#trx_sys" do
    it "should return a page for a system space" do
      @space.trx_sys.should be_an_instance_of Innodb::Page::TrxSys
    end

    it "should return nil for a non-system space" do
      @space_ibd.trx_sys.should eql nil
    end
  end

  describe "#data_dictionary_page" do
    it "should return a page for a system space" do
      @space.data_dictionary_page.should be_an_instance_of Innodb::Page::SysDataDictionaryHeader
    end

    it "should return nil for a non-system space" do
      @space_ibd.data_dictionary_page.should eql nil
    end
  end

  describe "#index" do
    it "is an Innodb::Index" do
      @space_ibd.index(3).should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_index" do
    it "is an enumerator" do
      is_enumerator?(@space_ibd.each_index).should be_true
    end

    it "iterates through indexes" do
      @space_ibd.each_index.to_a.size.should eql 1
    end

    it "yields an Innodb::Index" do
      @space_ibd.each_index.to_a.first.should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_page" do
    it "is an enumerator" do
      is_enumerator?(@space_ibd.each_page).should be_true
    end

    it "iterates through pages" do
      @space_ibd.each_page.to_a.size.should eql 6
    end

    it "yields an Array of [page_number, page]" do
      first_page = @space_ibd.each_page.to_a.first
      first_page.should be_an_instance_of Array
      first_page[0].should eql 0
      first_page[1].should be_an_instance_of Innodb::Page::FspHdrXdes
    end
  end

  describe "#each_xdes_page_number" do
    it "is an enumerator" do
      is_enumerator?(@space.each_xdes_page_number).should be_true
    end
  end

  describe "#xdes_page_for_page" do
    it "is a Fixnum" do
      @space.xdes_page_for_page(0).should be_an_instance_of Fixnum
    end

    it "calculates the correct page number" do
      @space.xdes_page_for_page(0).should eql 0
      @space.xdes_page_for_page(1).should eql 0
      @space.xdes_page_for_page(63).should eql 0
      @space.xdes_page_for_page(64).should eql 0
      @space.xdes_page_for_page(16383).should eql 0
      @space.xdes_page_for_page(16384).should eql 16384
      @space.xdes_page_for_page(32767).should eql 16384
      @space.xdes_page_for_page(32768).should eql 32768
    end
  end

  describe "#xdes_entry_for_page" do
    it "is a Fixnum" do
      @space.xdes_entry_for_page(0).should be_an_instance_of Fixnum
    end

    it "calculates the correct entry number" do
      @space.xdes_entry_for_page(0).should eql 0
      @space.xdes_entry_for_page(1).should eql 0
      @space.xdes_entry_for_page(63).should eql 0
      @space.xdes_entry_for_page(64).should eql 1
      @space.xdes_entry_for_page(65).should eql 1
      @space.xdes_entry_for_page(127).should eql 1
      @space.xdes_entry_for_page(128).should eql 2
      @space.xdes_entry_for_page(16383).should eql 255
      @space.xdes_entry_for_page(16384).should eql 0
      @space.xdes_entry_for_page(16385).should eql 0
      @space.xdes_entry_for_page(16448).should eql 1
      @space.xdes_entry_for_page(16511).should eql 1
      @space.xdes_entry_for_page(16512).should eql 2
      @space.xdes_entry_for_page(16576).should eql 3
      @space.xdes_entry_for_page(32767).should eql 255
    end
  end

  describe "#xdes_for_page" do
    it "is an Innodb::Xdes" do
      @space.xdes_for_page(0).should be_an_instance_of Innodb::Xdes
    end

    it "returns the correct XDES entry" do
      xdes = @space.xdes_for_page(0)
      (0 >= xdes.start_page).should eql true
      (0 <= xdes.end_page).should eql true
    end
  end

  describe "#each_xdes_page" do
    it "is an enumerator" do
      is_enumerator?(@space_ibd.each_xdes_page).should be_true
    end

    it "iterates through extent descriptor pages" do
      @space_ibd.each_xdes_page.to_a.size.should eql 1
    end

    it "yields an Innodb::Page::FspHdrXdes" do
      @space_ibd.each_xdes_page.to_a.first.should be_an_instance_of Innodb::Page::FspHdrXdes
    end
  end

  describe "#each_xdes" do
    it "is an enumerator" do
      is_enumerator?(@space_ibd.each_xdes).should be_true
    end

    it "iterates through extent descriptor entries" do
      @space_ibd.each_xdes.to_a.size.should eql 1
    end

    it "yields an Innodb::Xdes" do
      @space_ibd.each_xdes.to_a.first.should be_an_instance_of Innodb::Xdes
    end
  end

  describe "#each_page_type_region" do
    it "is an enumerator" do
      is_enumerator?(@space_ibd.each_page_type_region).should be_true
    end

    it "iterates through page type regions" do
      @space_ibd.each_page_type_region.to_a.size.should eql 5
    end

    it "yields a Hash with the right keys and values" do
      page_type_regions = @space_ibd.each_page_type_region.to_a

      page_type_regions[0].should be_an_instance_of Hash
      page_type_regions[0].size.should eql 4
      page_type_regions[0][:start].should eql 0
      page_type_regions[0][:end].should eql 0
      page_type_regions[0][:type].should eql :FSP_HDR
      page_type_regions[0][:count].should eql 1

      page_type_regions[1].should be_an_instance_of Hash
      page_type_regions[1].size.should eql 4
      page_type_regions[1][:start].should eql 1
      page_type_regions[1][:end].should eql 1
      page_type_regions[1][:type].should eql :IBUF_BITMAP
      page_type_regions[1][:count].should eql 1
    end
  end
end
