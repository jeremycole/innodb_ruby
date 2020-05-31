# frozen_string_literal: true

require 'spec_helper'

describe Innodb::Page::FspHdrXdes do
  before :all do
    @space = Innodb::Space.new('spec/data/t_empty.ibd')
    @page  = @space.page(0)
  end

  describe 'class' do
    it 'registers itself as a specialized page type' do
      Innodb::Page.specialization_for?(:FSP_HDR).should be_truthy
      Innodb::Page.specialization_for?(:XDES).should be_truthy
    end
  end

  describe '#new' do
    it 'returns an Innodb::Page::FspHdrXdes' do
      @page.should be_an_instance_of Innodb::Page::FspHdrXdes
    end

    it 'is an Innodb::Page' do
      @page.should be_a Innodb::Page
    end
  end

  describe '#each_list' do
    it 'returns an appropriate set of lists' do
      @page.each_list.map { |name, _| name }.should include(
        :free,
        :free_frag,
        :full_frag,
        :full_inodes,
        :free_inodes
      )
    end
  end
end
