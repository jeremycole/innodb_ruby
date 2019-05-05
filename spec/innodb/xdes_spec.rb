# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Xdes do
  before :all do
    @space = Innodb::Space.new("spec/data/ibdata1")
    @page  = @space.page(0)
    @cursor = @page.cursor(@page.pos_xdes_array)
    @xdes0 = Innodb::Xdes.new(@page, @cursor)
    @xdes1 = Innodb::Xdes.new(@page, @cursor)
  end

  describe "::STATES" do
    it "is a Hash" do
      Innodb::Xdes::STATES.should be_an_instance_of Hash
    end

    it "has only Integer keys" do
      classes = Innodb::Xdes::STATES.keys.map { |k| k.class }.uniq
      classes.should eql [Integer]
    end

    it "has only Symbol values" do
      classes = Innodb::Xdes::STATES.values.map { |v| v.class }.uniq
      classes.should eql [Symbol]
    end
  end

  describe "#new" do
    it "returns an Innodb::Xdes" do
      @xdes0.should be_an_instance_of Innodb::Xdes
    end
  end

  describe "#read_xdes_entry" do
    it "calculates the start_page correctly" do
      @xdes0.start_page.should eql 0
      @xdes1.start_page.should eql 64
    end

    it "calculates the end_page correctly" do
      @xdes0.end_page.should eql 63
      @xdes1.end_page.should eql 127
    end

    it "has the right methods and values" do
      @xdes0.start_page.should eql 0
      @xdes0.fseg_id.should eql 0
      @xdes0.this.should be_an_instance_of Hash
      @xdes0.list.should be_an_instance_of Hash
      @xdes0.bitmap.size.should eql 16
    end
  end

  describe "#xdes" do
    it "is a Hash" do
      @xdes0.xdes.should be_an_instance_of Hash
    end
  end

  describe "#allocated_to_fseg?" do
    it "works correctly" do
      @space.xdes_for_page(0).allocated_to_fseg?.should eql false
      @space.xdes_for_page(64).allocated_to_fseg?.should eql true
    end
  end

  describe "#page_status" do
    it "returns the status of a page" do
      status = @xdes0.page_status(0)
      status.should be_an_instance_of Hash
      status.size.should eql 2
      status[:free].should eql false
      status[:clean].should eql true
    end
  end

  describe "#each_page_status" do
    it "is an enumerator" do
      is_enumerator?(@xdes0.each_page_status).should be_truthy
    end

    it "yields Hashes" do
      @xdes0.each_page_status.to_a.map { |v| v[1].class }.uniq.should eql [Hash]
    end

    it "yields Hashes with the right keys and values" do
      status = @xdes0.each_page_status.to_a.first[1]
      status.size.should eql 2
      status[:free].should eql false
      status[:clean].should eql true
    end
  end

  describe "#free_pages" do
    it "returns the number of free pages" do
      @xdes0.free_pages.should eql 0
    end
  end

  describe "#used_pages" do
    it "returns the number of used pages" do
      @xdes0.used_pages.should eql 64
    end
  end

  describe "#==" do
    it "compares by page and offset" do
      (@xdes0 == @xdes1).should eql false
      (@xdes0 == @space.xdes_for_page(0)).should eql true
    end
  end
end
