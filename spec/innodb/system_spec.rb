# frozen_string_literal: true

require "spec_helper"

describe Innodb::System do
  before :all do
    @system = Innodb::System.new("spec/data/sakila/compact/ibdata1")
  end

  describe "data_directory" do
    it "cannot find tablespace files when the data directory is wrong" do
      broken_system = Innodb::System.new("spec/data/sakila/compact/ibdata1", data_directory: "foo/bar")
      space = broken_system.space_by_table_name("sakila/film")
      space.should be_nil
    end
  end

  describe "data_directory" do
    it "can find tablespace files using a specified data directory" do
      working_system = Innodb::System.new(
        "spec/data/sakila/compact/ibdata1",
        data_directory: "spec/data/sakila/compact"
      )
      space = working_system.space_by_table_name("sakila/film")
      space.should be_an_instance_of(Innodb::Space)
    end
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

  describe "#index_by_name" do
    it "returns an Innodb::Index object" do
      index = @system.index_by_name("sakila/film", "PRIMARY")
      index.should be_an_instance_of Innodb::Index
    end
  end

  describe "#each_orphan" do
    before :all do
      # Tablespace has a missing tablespace file for test/t_empty.
      @system = Innodb::System.new("spec/data/orphan/ibdata1")
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
