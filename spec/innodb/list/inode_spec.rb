# frozen_string_literal: true

require "spec_helper"

describe Innodb::List::Inode do
  before :all do
    @system = Innodb::System.new("spec/data/sakila/compact/ibdata1")
    @empty_list = @system.space_by_table_name("sakila/film").list(:full_inodes)
  end

  it "can read an empty list" do
    @empty_list.empty?.should(be_truthy)
  end

  it "only iterates through INODE pages" do
    @system.system_space.list(:full_inodes).each.map(&:type).should(eql(%i[INODE INODE]))
  end
end
