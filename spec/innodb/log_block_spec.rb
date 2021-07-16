# frozen_string_literal: true

require "spec_helper"

describe Innodb::LogBlock do
  before :all do
    @log = Innodb::Log.new("spec/data/ib_logfile0")
    @block = @log.block(0)
  end

  describe "#block" do
    it "has a correct checksum" do
      @block.checksum.should eql 1_706_444_976
    end

    it "is not corrupt" do
      @block.corrupt?.should eql false
    end

    it "returns a valid header" do
      h = @block.header
      h.flush.should eql true
      h.block_number.should eql 17
      h.data_length.should eql 512
      h.first_rec_group.should eql 12
      h.checkpoint_no.should eql 5
    end
  end
end
