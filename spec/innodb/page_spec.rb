# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Page do
  before :all do
    @space = Innodb::Space.new("spec/data/ibdata1")
    @page_data = @space.page_data(0)
    @page = @space.page(0)
  end

  describe "::PAGE_TYPE" do
    it "is a Hash" do
      Innodb::Page::PAGE_TYPE.should be_an_instance_of Hash
    end

    it "has only Symbol keys" do
      classes = Innodb::Page::PAGE_TYPE.keys.map { |k| k.class }.uniq
      classes.should eql [Symbol]
    end

    it "has only Hash values" do
      classes = Innodb::Page::PAGE_TYPE.values.map { |v| v.class }.uniq
      classes.should eql [Hash]
    end
  end

  describe "::PAGE_TYPE_BY_VALUE" do
    it "is a Hash" do
      Innodb::Page::PAGE_TYPE_BY_VALUE.should be_an_instance_of Hash
    end

    it "has only Integer keys" do
      classes = Innodb::Page::PAGE_TYPE_BY_VALUE.keys.map { |k| k.class }.uniq
      classes.should eql [Integer]
    end

    it "has only Symbol values" do
      classes = Innodb::Page::PAGE_TYPE_BY_VALUE.values.map { |v| v.class }.uniq
      classes.should eql [Symbol]
    end
  end

  describe "::SPECIALIZED_CLASSES" do
    it "is a Hash" do
      Innodb::Page::SPECIALIZED_CLASSES.should be_an_instance_of Hash
    end

    it "has only Symbol keys" do
      classes = Innodb::Page::SPECIALIZED_CLASSES.keys.map { |k| k.class }.uniq
      classes.should eql [Symbol]
    end

    it "has only keys that are keys in ::PAGE_TYPE" do
      checks = Innodb::Page::SPECIALIZED_CLASSES.keys.map { |k|
        Innodb::Page::PAGE_TYPE.include? k
      }.uniq
      checks.should eql [true]
    end

    it "has only Class values" do
      classes = Innodb::Page::SPECIALIZED_CLASSES.values.map { |v| v.class }.uniq
      classes.should eql [Class]
    end

    it "has only values subclassing Innodb::Page" do
      classes = Innodb::Page::SPECIALIZED_CLASSES.values.map { |v| v.superclass }.uniq
      classes.should eql [Innodb::Page]
    end
  end

  describe "#new" do
    it "returns a class" do
      Innodb::Page.new(@space, @page_data).should be_an_instance_of Innodb::Page    
    end
  end

  describe "#parse" do
    it "delegates to the right specialized class" do
      Innodb::Page.parse(@space, @page_data).should be_an_instance_of Innodb::Page::FspHdrXdes    
    end
  end

  describe "#cursor" do
    it "returns a cursor" do
      @page.cursor(0).should be_an_instance_of BufferCursor
    end

    it "positions the cursor correctly" do
      @page.cursor(0).position.should eql 0
      @page.cursor(4).position.should eql 4
    end

    it "is reading reasonable data" do
      # This will read the page number from page 0, which should be 0.
      @page.cursor(4).get_uint32.should eql 0
    end
  end

  describe "#maybe_undefined" do
    it "returns the value when the value is not UINT_MAX" do
      Innodb::Page.maybe_undefined(5).should eql 5
    end
    
    it "returns nil when the value is UINT_MAX" do
      Innodb::Page.maybe_undefined(4294967295).should eql nil
    end
  end

  describe "#fil_header" do
    it "returns a Hash" do
      @page.fil_header.should be_an_instance_of Hash
    end
    
    it "has only Symbol keys" do
      classes = @page.fil_header.keys.map { |k| k.class }.uniq
      classes.should eql [Symbol]
    end

    it "has the right keys and values" do
      @page.fil_header.size.should eql 8
      @page.fil_header[:checksum].should eql 2067631406
      @page.fil_header[:offset].should eql 0
      @page.fil_header[:prev].should eql 0
      @page.fil_header[:next].should eql 0
      @page.fil_header[:lsn].should eql 1601269
      @page.fil_header[:type].should eql :FSP_HDR
      @page.fil_header[:flush_lsn].should eql 1603732
      @page.fil_header[:space_id].should eql 0
    end

    it "has working helper functions" do
      @page.checksum.should eql @page.fil_header[:checksum]
      @page.offset.should eql @page.fil_header[:offset]
      @page.prev.should eql @page.fil_header[:prev]
      @page.next.should eql @page.fil_header[:next]
      @page.lsn.should eql @page.fil_header[:lsn]
      @page.type.should eql @page.fil_header[:type]
    end
  end

  describe "#checksum_innodb" do
    it "calculates the right checksum" do
      @page.checksum_innodb.should eql 2067631406
      @page.corrupt?.should eql false
    end
  end
end
