# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Page::Inode do
  before :all do
    @space = Innodb::Space.new("spec/data/ibdata1")
    @page  = @space.page(2)
  end

  describe "class" do
    it "registers itself in Innodb::Page::SPECIALIZED_CLASSES" do
      Innodb::Page::SPECIALIZED_CLASSES[:INODE].should eql Innodb::Page::Inode
    end
  end

  describe "#new" do
    it "returns an Innodb::Page::Inode" do
      @page.should be_an_instance_of Innodb::Page::Inode
    end

    it "is an Innodb::Page" do
      @page.should be_a Innodb::Page
    end
  end

  describe "#list_entry" do
    it "is a Hash" do
      @page.list_entry.should be_an_instance_of Hash
    end

    it "has the right keys and values" do
      @page.list_entry.size.should eql 2
      @page.list_entry[:prev].should eql nil
      @page.list_entry[:next].should eql nil
    end

    it "has helper functions" do
      @page.prev_address.should eql @page.list_entry[:prev]
      @page.next_address.should eql @page.list_entry[:next]
    end
  end

  describe "#each_inode" do
    it "yields Innodb::Inode objects" do
      @page.each_inode.to_a.map { |v| v.class }.uniq.should eql [Innodb::Inode]
    end

    it "yields Hashes with the right keys and values" do
      inode = @page.each_inode.to_a.first
      inode.fseg_id.should eql 1
      inode.not_full_n_used.should eql 0
      inode.free.should be_an_instance_of Innodb::List::Xdes
      inode.not_full.should be_an_instance_of Innodb::List::Xdes
      inode.full.should be_an_instance_of Innodb::List::Xdes
      inode.magic_n.should eql Innodb::Inode::MAGIC_N_VALUE
      inode.frag_array.should be_an_instance_of Array
    end
  end

  describe "#each_allocated_inode" do
    it "yields Innodb::Inode objects" do
      @page.each_allocated_inode.to_a.map { |v| v.class }.uniq.should eql [Innodb::Inode]
    end

    it "yields only allocated inodes" do
      @page.each_allocated_inode do |inode|
        inode.allocated?.should be_truthy
      end
    end
  end
end
