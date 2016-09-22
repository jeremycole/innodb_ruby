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
      Innodb::Stats.reset
      rec = @index.linear_search([5000])
      linear_compares = Innodb::Stats.get(:compare_key)

      Innodb::Stats.reset
      rec = @index.binary_search([5000])
      binary_compares = Innodb::Stats.get(:compare_key)

      ((linear_compares.to_f / binary_compares.to_f) > 10).should be_truthy
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

  describe "#min_page_at_level" do
    it "returns the min page" do
      page = @index.min_page_at_level(0)
      page.level.should eql 0
      rec = page.min_record
      rec.key[0][:value].should eql 1
    end
  end

  describe "#min_record" do
    it "returns the min record" do
      rec = @index.min_record
      rec.key[0][:value].should eql 1
    end
  end

  describe "#max_page_at_level" do
    it "returns the max page" do
      page = @index.max_page_at_level(0)
      page.level.should eql 0
      rec = page.max_record
      rec.key[0][:value].should eql 10000
    end
  end

  describe "#max_record" do
    it "returns the max record" do
      rec = @index.max_record
      rec.key[0][:value].should eql 10000
    end
  end

  describe "#cursor" do
    it "returns an Innodb::Index::IndexCursor" do
      @index.cursor.should be_an_instance_of Innodb::Index::IndexCursor
    end
  end

  describe Innodb::Index::IndexCursor do
    describe "#record" do
      it "iterates in forward order" do
        cursor = @index.cursor(:min, :forward)

        previous = cursor.record
        100.times do
          current = cursor.record
          (current.key[0][:value].to_i > previous.key[0][:value].to_i).should be_truthy
          previous = current
        end
      end

      it "iterates in backward order" do
        cursor = @index.cursor(:max, :backward)

        previous = cursor.record
        100.times do
          current = cursor.record
          (current.key[0][:value].to_i < previous.key[0][:value].to_i).should be_truthy
          previous = current
        end
      end

      it "iterates across page boundaries" do
        cursor = @index.cursor

        # This will be the first record, from page 4.
        rec = cursor.record
        rec.page.offset.should eql 4

        # Skip 900 records.
        900.times { cursor.record.should_not be_nil }

        # We should have crossed a page boundary.
        rec = cursor.record
        rec.page.offset.should_not eql 4
      end

      it "iterates back and forth" do
        cursor = @index.cursor(:min, :forward)

        1.upto(900) do |v|
          cursor.record.key[0][:value].to_i.should eql v
        end

        cursor = @index.cursor(cursor.record, :backward)

        901.downto(1) do |v|
          cursor.record.key[0][:value].to_i.should eql v
        end

        cursor.record.should be_nil
      end

      it "handles index bounds" do
        cursor = @index.cursor(:min, :backward)
        cursor.record.key[0][:value].to_i.should eql 1
        cursor.record.should be_nil

        cursor = @index.cursor(:max, :forward)
        cursor.record.key[0][:value].to_i.should eql 10000
        cursor.record.should be_nil
      end
    end

    describe "#each_record" do
      it "is an enumerator" do
        is_enumerator?(@index.cursor.each_record).should be_truthy
      end
    end
  end
end

