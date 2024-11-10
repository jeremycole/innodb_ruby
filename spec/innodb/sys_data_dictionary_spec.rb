# frozen_string_literal: true

require "spec_helper"

describe Innodb::SysDataDictionary do
  before :all do
    @system = Innodb::System.new("spec/data/sakila/compact/ibdata1")
    @dict = @system.data_dictionary
  end

  describe "#mtype_prtype_to_type_string" do
    it "produces the correct type string or symbol" do
      type = Innodb::SysDataDictionary.mtype_prtype_to_type_string(6, 1794, 2, 0)
      type.should eql :SMALLINT
    end
  end

  describe "#mtype_prtype_to_data_type" do
    it "produces the correct type array" do
      type = Innodb::SysDataDictionary.mtype_prtype_to_data_type(6, 1794, 2, 0)
      type.should be_an_instance_of Array
      type.should eql %i[SMALLINT NOT_NULL UNSIGNED]
    end
  end

  # TODO: Write new tests for SysDataDictionary
end
