# frozen_string_literal: true

require "spec_helper"

describe Innodb::Checksum do
  describe "#fold_pair" do
    it "returns a Integer" do
      Innodb::Checksum.fold_pair(0x00, 0x00).should be_an_instance_of Integer
    end

    it "calculates correct values" do
      Innodb::Checksum.fold_pair(0x00, 0x00).should eql 3_277_101_703
      Innodb::Checksum.fold_pair(0x00, 0xff).should eql 3_277_088_390
      Innodb::Checksum.fold_pair(0xff, 0x00).should eql 3_277_088_120
      Innodb::Checksum.fold_pair(0xff, 0xff).should eql 3_277_101_943
    end
  end

  describe "#fold_enumerator" do
    it "returns a Integer" do
      Innodb::Checksum.fold_enumerator(0..255).should be_an_instance_of Integer
    end

    it "calculates correct values" do
      Innodb::Checksum.fold_enumerator(0..255).should eql 1_406_444_672
    end
  end

  describe "#fold_string" do
    it "returns a Integer" do
      Innodb::Checksum.fold_string("hello world").should be_an_instance_of Integer
    end

    it "calculates correct values" do
      Innodb::Checksum.fold_string("hello world").should eql 2_249_882_843
    end
  end
end
