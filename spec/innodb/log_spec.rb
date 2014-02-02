# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::Log do
  before :all do
    @log = Innodb::Log.new("spec/data/ib_logfile0")
  end

  describe "#new" do
    it "defines a class" do
      Innodb::Log.should be_an_instance_of Class
    end

    it "returns an Innodb::Log" do
      @log.should be_an_instance_of Innodb::Log
    end
  end

  describe "#size" do
    it "returns 5242880 bytes" do
      @log.size.should eql 5242880
    end
  end

  describe "#blocks" do
    it "returns 10236 blocks" do
      @log.blocks.should eql 10236
    end
  end

  describe "#block" do
    it "returns an Innodb::Block" do
      @log.block(0).should be_an_instance_of Innodb::LogBlock
    end
  end
end
