# -*- encoding : utf-8 -*-

require 'spec_helper'

describe Innodb::DataDictionary do
  before :all do
    @system = Innodb::System.new("spec/data/sakila/compact/ibdata1")
    @dict = @system.data_dictionary
  end

  describe "#mtype_prtype_to_type_string" do
    it "produces the correct type string or symbol" do
      type = Innodb::DataDictionary.mtype_prtype_to_type_string(6, 1794, 2, 0)
      type.should eql :SMALLINT
    end
  end

  describe "#mtype_prtype_to_data_type" do
    it "produces the correct type array" do
      type = Innodb::DataDictionary.mtype_prtype_to_data_type(6, 1794, 2, 0)
      type.should be_an_instance_of Array
      type.should eql [:SMALLINT, :NOT_NULL, :UNSIGNED]
    end
  end

  describe "#data_dictionary_indexes" do
    it "is a Hash" do
      @dict.data_dictionary_indexes.should be_an_instance_of Hash
    end

    it "contains Hashes" do
      key = @dict.data_dictionary_indexes.keys.first
      @dict.data_dictionary_indexes[key].should be_an_instance_of Hash
    end
  end

  describe "#data_dictionary_index" do
    it "returns Innodb::Index objects" do
      index = @dict.data_dictionary_index(:SYS_TABLES, :PRIMARY)
      index.should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_data_dictionary_index_root_page_number" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_data_dictionary_index_root_page_number).should be_truthy
    end
  end

  describe "#each_data_dictionary_index" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_data_dictionary_index).should be_truthy
    end
  end

  describe "#each_record_from_data_dictionary_index" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_record_from_data_dictionary_index(:SYS_TABLES, :PRIMARY)).should be_truthy
    end
  end

  describe "#each_table" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_table).should be_truthy
    end
  end

  describe "#each_column" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_column).should be_truthy
    end
  end

  describe "#each_index" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_index).should be_truthy
    end
  end

  describe "#each_field" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_field).should be_truthy
    end
  end

  describe "#table_by_id" do
    it "finds the correct table" do
      table = @dict.table_by_id(19)
      table.should be_an_instance_of Hash
      table["NAME"].should eql "sakila/film"
    end
  end

  describe "#table_by_name" do
    it "finds the correct table" do
      table = @dict.table_by_name("sakila/film")
      table.should be_an_instance_of Hash
      table["NAME"].should eql "sakila/film"
    end
  end

  describe "#table_by_space_id" do
    it "finds the correct table" do
      table = @dict.table_by_space_id(7)
      table.should be_an_instance_of Hash
      table["NAME"].should eql "sakila/film"
    end
  end

  describe "#column_by_name" do
    it "finds the correct column" do
      column = @dict.column_by_name("sakila/film", "film_id")
      column.should be_an_instance_of Hash
      column["NAME"].should eql "film_id"
    end
  end

  describe "#index_by_id" do
    it "finds the correct index" do
      index = @dict.index_by_id(27)
      index.should be_an_instance_of Hash
      index["NAME"].should eql "PRIMARY"
    end
  end

  describe "#index_by_name" do
    it "finds the correct index" do
      index = @dict.index_by_name("sakila/film", "PRIMARY")
      index.should be_an_instance_of Hash
      index["NAME"].should eql "PRIMARY"
    end
  end

  describe "#each_index_by_space_id" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_index_by_space_id(7)).should be_truthy
    end
  end

  describe "#each_index_by_table_id" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_index_by_table_id(19)).should be_truthy
    end
  end

  describe "#each_index_by_table_name" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_index_by_table_name("sakila/film")).should be_truthy
    end
  end

  describe "#each_field_by_index_id" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_field_by_index_id(27)).should be_truthy
    end
  end

  describe "#each_field_by_index_name" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_field_by_index_name("sakila/film", "PRIMARY")).should be_truthy
    end
  end

  describe "#each_column_by_table_id" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_column_by_table_id(19)).should be_truthy
    end
  end

  describe "#each_column_by_table_name" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_column_by_table_name("sakila/film")).should be_truthy
    end
  end

  describe "#each_column_in_index_by_name" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_column_in_index_by_name("sakila/film", "PRIMARY")).should be_truthy
    end
  end

  describe "#each_column_not_in_index_by_name" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_column_not_in_index_by_name("sakila/film", "PRIMARY")).should be_truthy
    end
  end

  describe "#clustered_index_name_by_table_name" do
  end

  describe "#each_column_description_by_index_name" do
    it "is an enumerator" do
      is_enumerator?(@dict.each_column_description_by_index_name("sakila/film", "PRIMARY")).should be_truthy
    end
  end

  describe "#record_describer_by_index_name" do
    it "returns an Innodb::RecordDescriber" do
      desc = @dict.record_describer_by_index_name("sakila/film", "PRIMARY")
      desc.should be_an_instance_of Innodb::RecordDescriber
    end
  end

  describe "#record_describer_by_index_id" do
    it "returns an Innodb::RecordDescriber" do
      desc = @dict.record_describer_by_index_id(27)
      desc.should be_an_instance_of Innodb::RecordDescriber
    end
  end
end
