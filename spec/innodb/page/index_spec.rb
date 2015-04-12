# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Page::Index do
  before :all do
    @space = Innodb::Space.new("spec/data/t_empty.ibd")
    @page  = @space.page(3)
  end

  describe "class" do
    it "registers itself in Innodb::Page::SPECIALIZED_CLASSES" do
      Innodb::Page::SPECIALIZED_CLASSES[:INDEX].should eql Innodb::Page::Index
    end
  end

  describe "#new" do
    it "returns an Innodb::Page::Index" do
      @page.should be_an_instance_of Innodb::Page::Index::Uncompressed
    end

    it "is an Innodb::Page" do
      @page.should be_a Innodb::Page
    end
  end

  describe "#page_header" do
    it "is a Hash" do
      @page.page_header.should be_an_instance_of Hash
    end

    it "has the right keys and values" do
      @page.page_header.keys.size.should eql 13
      @page.page_header[:n_dir_slots].should eql 2
      @page.page_header[:heap_top].should eql 120
      @page.page_header[:garbage_offset].should eql 0
      @page.page_header[:garbage_size].should eql 0
      @page.page_header[:last_insert_offset].should eql 0
      @page.page_header[:direction].should eql :no_direction
      @page.page_header[:n_direction].should eql 0
      @page.page_header[:n_recs].should eql 0
      @page.page_header[:max_trx_id].should eql 0
      @page.page_header[:level].should eql 0
      @page.page_header[:index_id].should eql 16
      @page.page_header[:n_heap].should eql 2
      @page.page_header[:format].should eql :compact
    end
  
    it "has helper functions" do
      @page.level.should eql @page.page_header[:level]
      @page.records.should eql @page.page_header[:n_recs]
      @page.directory_slots.should eql @page.page_header[:n_dir_slots]
      @page.root?.should eql true
    end
  end

  describe "#free_space" do
    it "returns the amount of free space" do
      @page.free_space.should eql 16252
    end
  end

  describe "#used_space" do
    it "returns the amount of used space" do
      @page.used_space.should eql 132
    end
  end

  describe "#record_space" do
    it "returns the amount of record space" do
      @page.record_space.should eql 0
    end
  end

  describe "#fseg_header" do
    it "is a Hash" do
      @page.fseg_header.should be_an_instance_of Hash
    end
    
    it "has the right keys and values" do
      @page.fseg_header.keys.size.should eql 2
      @page.fseg_header[:leaf].should be_an_instance_of Innodb::Inode
      @page.fseg_header[:internal].should be_an_instance_of Innodb::Inode
    end
  end

  describe "#record_header" do
    before :all do
      @header = @page.record_header(@page.cursor(@page.pos_infimum))
    end

    it "is a Hash" do
      @header.should be_an_instance_of Hash
    end
    
    it "has the right keys and values" do
      @header.size.should eql 7
      @header[:type].should eql :infimum
      @header[:next].should eql 112
      @header[:heap_number].should eql 0
      @header[:n_owned].should eql 1
      @header[:min_rec].should eql false
      @header[:deleted].should eql false
    end
  end

  describe "#system_record" do
    it "can read infimum" do
      rec = @page.infimum
      rec.should be_an_instance_of Innodb::Record
      rec.record[:data].should eql "infimum\x00"
      rec.header.should be_an_instance_of Hash
      rec.header[:type].should eql :infimum
    end

    it "can read supremum" do
      rec = @page.supremum
      rec.should be_an_instance_of Innodb::Record
      rec.record[:data].should eql "supremum"
      rec.header.should be_an_instance_of Hash
      rec.header[:type].should eql :supremum
    end
  end

  describe "#record_cursor" do
    it "returns an Innodb::Page::Index::RecordCursor" do
      @page.record_cursor.should be_an_instance_of Innodb::Page::Index::Uncompressed::RecordCursor
    end
  end
end
