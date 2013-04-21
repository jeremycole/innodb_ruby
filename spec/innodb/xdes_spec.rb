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

    it "has only Fixnum keys" do
      classes = Innodb::Xdes::STATES.keys.map { |k| k.class }.uniq
      classes.should eql [Fixnum]
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
      @xdes0.xdes[:start_page].should eql 0
      @xdes1.xdes[:start_page].should eql 64
    end

    it "has the right keys and values" do
      @xdes0.xdes[:start_page].should eql 0
      @xdes0.xdes[:fseg_id].should eql 0
      @xdes0.xdes[:this].should be_an_instance_of Hash
      @xdes0.xdes[:list].should be_an_instance_of Hash
      @xdes0.xdes[:bitmap].size.should eql 16
    end
  end

  describe "#xdes" do
    it "is a Hash" do
      @xdes0.xdes.should be_an_instance_of Hash
    end
  end

  describe "#each_page_status" do
    it "is an Enumerator" do
      @xdes0.each_page_status.should be_an_instance_of Enumerator
    end

    it "yields Hashes" do
      @xdes0.each_page_status.to_a.map { |v| v.class }.uniq.should eql [Hash]
    end

    it "yields Hashes with the right keys and values" do
      status = @xdes0.each_page_status.to_a.first
      status.size.should eql 3
      status[:page].should eql 0
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
end