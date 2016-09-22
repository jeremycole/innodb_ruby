# -*- encoding : utf-8 -*-

require 'spec_helper'

describe Innodb::System do
  before :all do
    @system = Innodb::System.new("spec/data/sakila/compact/ibdata1")
  end

  describe "#system_space" do
    it "returns an Innodb::Space" do
      @system.system_space.should be_an_instance_of Innodb::Space
    end

    it "returns space 0" do
      @system.system_space.space_id.should eql 0
    end
  end

  describe "#add_space" do
    it "adds a space to the system" do
      sys = Innodb::System.new("spec/data/sakila/compact/ibdata1")
      space = Innodb::Space.new("spec/data/sakila/compact/sakila/film.ibd")
      sys.add_space(space)

      sys.spaces.keys.include?(7).should be_truthy
    end
  end

  describe "#add_space_file" do
    it "adds a space to the system" do
      sys = Innodb::System.new("spec/data/sakila/compact/ibdata1")
      sys.add_space_file("spec/data/sakila/compact/sakila/film.ibd")

      sys.spaces.keys.include?(7).should be_truthy
    end
  end

  describe "#add_table" do
    it "adds a space to the system" do
      sys = Innodb::System.new("spec/data/sakila/compact/ibdata1")
      sys.add_table("sakila/film")

      sys.spaces.keys.include?(7).should be_truthy
    end
  end

  describe "#space_by_table_name" do
    it "returns an Innodb::Space" do
      space = @system.space_by_table_name("sakila/film")
      space.should be_an_instance_of Innodb::Space
    end
  end

  describe "#space" do
    it "returns an Innodb::Space" do
      sys = Innodb::System.new("spec/data/sakila/compact/ibdata1")
      sys.add_table("sakila/film")
      sys.space(7).should be_an_instance_of Innodb::Space
    end
  end

  describe "#space_by_table_name" do
    it "returns an Innodb::Space" do
      sys = Innodb::System.new("spec/data/sakila/compact/ibdata1")
      sys.add_table("sakila/film")
      sys.space_by_table_name("sakila/film").should be_an_instance_of Innodb::Space
    end
  end

  describe "#each_table_name" do
    it "is an enumerator" do
      is_enumerator?(@system.each_table_name).should be_truthy
    end

    it "iterates all tables in the system" do
      expected = [
        "SYS_FOREIGN",
        "SYS_FOREIGN_COLS",
        "sakila/actor",
        "sakila/address",
        "sakila/category",
        "sakila/city",
        "sakila/country",
        "sakila/customer",
        "sakila/film",
        "sakila/film_actor",
        "sakila/film_category",
        "sakila/inventory",
        "sakila/language",
        "sakila/payment",
        "sakila/rental",
        "sakila/staff",
        "sakila/store",
      ]

      actual = @system.each_table_name.to_a
      result = actual.map { |n| expected.include?(n) }.uniq
      result.should eql [true]
    end
  end

  describe "#each_column_name_by_table_name" do
    it "is an enumerator" do
      is_enumerator?(@system.each_column_name_by_table_name("sakila/film")).should be_truthy
    end

    it "iterates all columns in the table" do
      expected = [
        "film_id",
        "title",
        "description",
        "release_year",
        "language_id",
        "original_language_id",
        "rental_duration",
        "rental_rate",
        "length",
        "replacement_cost",
        "rating",
        "special_features",
        "last_update",
      ]

      actual = @system.each_column_name_by_table_name("sakila/film").to_a
      result = actual.map { |n| expected.include?(n) }.uniq
      result.should eql [true]
    end
  end

  describe "#each_index_name_by_table_name" do
    it "is an enumerator" do
      is_enumerator?(@system.each_index_name_by_table_name("sakila/film")).should be_truthy
    end

    it "iterates all indexes in the table" do
      expected = [
        "PRIMARY",
        "idx_title",
        "idx_fk_language_id",
        "idx_fk_original_language_id",
      ]

      actual = @system.each_index_name_by_table_name("sakila/film").to_a
      result = actual.map { |n| expected.include?(n) }.uniq
      result.should eql [true]
    end
  end

  describe "#table_name_by_id" do
    it "returns the correct table name" do
      @system.table_name_by_id(19).should eql "sakila/film"
    end
  end

  describe "#index_name_by_id" do
    it "returns the correct index name" do
      @system.index_name_by_id(27).should eql "PRIMARY"
    end
  end

  describe "#table_and_index_name_by_id" do
    it "returns the correct table and index name" do
      table, index = @system.table_and_index_name_by_id(27)
      table.should eql "sakila/film"
      index.should eql "PRIMARY"
    end
  end

  describe "#index_by_name" do
    it "returns an Innodb::Index object" do
      index = @system.index_by_name("sakila/film", "PRIMARY")
      index.should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_orphan" do
    before :all do
      @system = Innodb::System.new("spec/data/ibdata1")
    end

    it "has an orphan space" do
      @system.space_by_table_name("test/t_empty").should be_nil
    end

    it "is an enumerator" do
      is_enumerator?(@system.each_orphan).should be_truthy
    end

    it "returns the orphan space" do
      @system.each_orphan.next.should eql "test/t_empty"
    end
  end

end
