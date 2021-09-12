# frozen_string_literal: true

require "spec_helper"

describe Innodb::List::History do
  before :all do
    @system = Innodb::System.new("spec/data/sakila/compact/ibdata1")
    @empty_list = @system.history.each_history_list.first&.list
  end

  it "can read an empty list" do
    @empty_list.empty?.should(be_truthy)
  end
end
