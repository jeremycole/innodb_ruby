# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Page::FspHdrXdes do
  before :all do
    @space = Innodb::Space.new("spec/data/t_empty.ibd")
    @page  = @space.page(0)
  end

  describe "class" do
    it "registers itself in Innodb::Page::SPECIALIZED_CLASSES" do
      Innodb::Page::SPECIALIZED_CLASSES[:FSP_HDR].should eql Innodb::Page::FspHdrXdes
      Innodb::Page::SPECIALIZED_CLASSES[:XDES].should eql Innodb::Page::FspHdrXdes
    end
  end

  describe "#new" do
    it "returns an Innodb::Page::FspHdrXdes" do
      @page.should be_an_instance_of Innodb::Page::FspHdrXdes
    end

    it "is an Innodb::Page" do
      @page.should be_a Innodb::Page
    end
  end
end
