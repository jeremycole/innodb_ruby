# -*- encoding : utf-8 -*-
require 'spec_helper'

describe Innodb::LogBlock do
  before :all do
    @block = Innodb::Log.new("spec/data/ib_logfile0").block(0)
  end

  subject { @block }

  its(:checksum) { should eql 1706444976 }
  its(:corrupt?) { should eql false }
  its(:header) do
    should eql(
      :flush            => true,
      :block_number     => 17,
      :data_length      => 512,
      :first_rec_group  => 12,
      :checkpoint_no    => 5)
  end
end
