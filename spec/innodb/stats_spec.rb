# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Stats do
  before :each do
    Innodb::Stats.reset
  end

  describe "#data" do
    it "is a Hash" do
      Innodb::Stats.data.should be_an_instance_of Hash
    end
  end

  describe "#get" do
    it "returns 0 for an unused statistic" do
      Innodb::Stats.get(:foo).should eql 0
    end

    it "gets the statistic" do
      Innodb::Stats.increment :foo
      Innodb::Stats.get(:foo).should eql 1
    end
  end

  describe "#increment" do
    it "increments the statistic" do
      Innodb::Stats.get(:foo).should eql 0
      Innodb::Stats.increment :foo
      Innodb::Stats.get(:foo).should eql 1
    end
  end

  describe "#reset" do
    it "resets the statistics" do
      Innodb::Stats.data.size.should eql 0
      Innodb::Stats.increment :foo
      Innodb::Stats.data.size.should eql 1
      Innodb::Stats.reset
      Innodb::Stats.data.size.should eql 0
    end
  end

end
