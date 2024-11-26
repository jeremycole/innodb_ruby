# frozen_string_literal: true

require "spec_helper"

describe Innodb::MysqlCollation do
  describe ".collations" do
    it "is an Array" do
      Innodb::MysqlCollation.collations.should be_an_instance_of Array
    end

    # The count/minmax here can be updated when lib/innodb/mysql_collations.rb is updated, but it should probably never
    # decrease, only increase. Proceed with caution.

    it "has 287 entries" do
      Innodb::MysqlCollation.collations.count.should eql 287
    end

    it "has entries from id 1 to 323" do
      Innodb::MysqlCollation.collations.map(&:id).minmax.should eql [1, 323]
    end

    it "has only Innodb::MysqlCollation entries" do
      Innodb::MysqlCollation.collations.map(&:class).uniq.should eql [Innodb::MysqlCollation]
    end

    it "has mbminlen values of 1, 2, 4" do
      Innodb::MysqlCollation.collations.map(&:mbminlen).sort.uniq.should eql [1, 2, 4]
    end

    it "has mbmaxlen values of 1, 2, 3, 4, 5" do
      Innodb::MysqlCollation.collations.map(&:mbmaxlen).sort.uniq.should eql [1, 2, 3, 4, 5]
    end
  end

  describe ".by_id" do
    it "can look up utf8mb4_general_ci by id" do
      Innodb::MysqlCollation.by_id(45).name.should eql "utf8mb4_general_ci"
    end
  end

  describe ".by_name" do
    it "can look up utf8mb4_general_ci by name" do
      Innodb::MysqlCollation.by_name("utf8mb4_general_ci").id.should eql 45
    end
  end

  describe "#fixed?" do
    it "works properly for two example collations" do
      Innodb::MysqlCollation.by_name("ascii_general_ci").fixed?.should be true
      Innodb::MysqlCollation.by_name("utf8mb4_general_ci").fixed?.should be false
    end
  end

  describe "#variable?" do
    it "works properly for two example collations" do
      Innodb::MysqlCollation.by_name("ascii_general_ci").variable?.should be false
      Innodb::MysqlCollation.by_name("utf8mb4_general_ci").variable?.should be true
    end
  end
end
