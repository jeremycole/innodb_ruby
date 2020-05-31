# frozen_string_literal: true

require 'spec_helper'

describe Innodb::Page::TrxSys do
  before :all do
    @space = Innodb::Space.new('spec/data/ibdata1')
    @page = @space.page(5)
  end

  describe 'class' do
    it 'registers itself as a specialized page type' do
      Innodb::Page.specialization_for?(:TRX_SYS).should be_truthy
    end
  end

  describe '#new' do
    it 'returns an Innodb::Page::TrxSys' do
      @page.should be_an_instance_of Innodb::Page::TrxSys
    end

    it 'is an Innodb::Page' do
      @page.should be_a Innodb::Page
    end
  end

  describe '#trx_sys' do
    it 'is a Innodb::Page::TrxSys::Header' do
      @page.trx_sys.should be_an_instance_of Innodb::Page::TrxSys::Header
    end

    it 'has the right keys and values' do
      @page.trx_sys.trx_id.should eql 1280
      @page.trx_sys.rsegs.should be_an_instance_of Array
      @page.trx_sys.binary_log.should eql nil
      @page.trx_sys.master_log.should eql nil
      @page.trx_sys.doublewrite.should be_an_instance_of Innodb::Page::TrxSys::DoublewriteInfo
    end
  end
end
