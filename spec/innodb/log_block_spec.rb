# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::LogBlock do
  before :all do
    @log = Innodb::Log.new("spec/data/ib_logfile0")
    @block = @log.block(0)
  end

  describe "#block" do
    it "has a correct checksum" do
      @block.checksum.should eql 1706444976
    end

    it "is not corrupt" do
      @block.corrupt?.should eql false
    end

    it "returns a valid header" do
      @block.header.should eql({
        :flush            => true,
        :block_number     => 17,
        :data_length      => 512,
        :first_rec_group  => 12,
        :checkpoint_no    => 5,
      })
    end
  end
end
