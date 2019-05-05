# -*- encoding : utf-8 -*-

require "spec_helper"

describe Innodb::Checksum do
  describe "#fold_pair" do
    it "returns a Integer" do
      Innodb::Checksum.fold_pair(0x00, 0x00).should be_an_instance_of Integer
    end

    it "calculates correct values" do
      Innodb::Checksum.fold_pair(0x00, 0x00).should eql 3277101703
      Innodb::Checksum.fold_pair(0x00, 0xff).should eql 3277088390
      Innodb::Checksum.fold_pair(0xff, 0x00).should eql 3277088120
      Innodb::Checksum.fold_pair(0xff, 0xff).should eql 3277101943
    end
  end

  describe "#fold_enumerator" do
    it "returns a Integer" do
      Innodb::Checksum.fold_enumerator(0..255).should be_an_instance_of Integer
    end

    it "calculates correct values" do
      Innodb::Checksum.fold_enumerator(0..255).should eql 1406444672
    end
  end

  describe "#fold_string" do
    it "returns a Integer" do
      Innodb::Checksum.fold_string("hello world").should be_an_instance_of Integer
    end

    it "calculates correct values" do
      Innodb::Checksum.fold_string("hello world").should eql 2249882843
    end
  end
end
