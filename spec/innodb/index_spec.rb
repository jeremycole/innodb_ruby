# -*- encoding : utf-8 -*-

require "spec_helper"

class TTenKRowsDescriber < Innodb::RecordDescriber
    type :clustered
      key "i", :INT, :UNSIGNED, :NOT_NULL
end

describe Innodb::Index do
  before :all do
    @space = Innodb::Space.new("spec/data/t_10k_rows.ibd")
    @space.record_describer = TTenKRowsDescriber.new
    @index = @space.index(3)
  end

  describe "#linear_search" do
    it "finds the correct row" do
      rec = @index.linear_search([500])
      rec.key[0][:value].should eql 500
    end

    it "handles failed searches" do
      rec = @index.linear_search([999999])
      rec.should be_nil
    end

    it "can find boundary rows" do
      rec = @index.linear_search([1])
      rec.key[0][:value].should eql 1

      rec = @index.linear_search([10000])
      rec.key[0][:value].should eql 10000

      rec = @index.linear_search([0])
      rec.should be_nil

      rec = @index.linear_search([10001])
      rec.should be_nil
    end
  end

  describe "#binary_search" do
    it "finds the correct row" do
      rec = @index.binary_search([500])
      rec.key[0][:value].should eql 500
    end

    it "handles failed searches" do
      rec = @index.binary_search([999999])
      rec.should be_nil
    end

    it "can find boundary rows" do
      rec = @index.binary_search([1])
      rec.key[0][:value].should eql 1

      rec = @index.binary_search([10000])
      rec.key[0][:value].should eql 10000

      rec = @index.binary_search([0])
      rec.should be_nil

      rec = @index.binary_search([10001])
      rec.should be_nil
    end

    it "is much more efficient than linear_search" do
      @index.reset_stats
      rec = @index.linear_search([5000])
      linear_compares = @index.stats[:compare_key]

      @index.reset_stats
      rec = @index.binary_search([5000])
      binary_compares = @index.stats[:compare_key]

      ((linear_compares.to_f / binary_compares.to_f) > 10).should be_true
    end

    it "can find 200 random rows" do
      missing_keys = {}
      (200.times.map { (rand() * 10000 + 1).floor }).map do |i|
        rec = @index.binary_search([i])
        if rec.nil?
          missing_keys[i] = :missing_key
        elsif rec.key[0][:value] != i
          missing_keys[i] = :mismatched_value
        end
      end
      missing_keys.should eql({})
    end
  end
end

