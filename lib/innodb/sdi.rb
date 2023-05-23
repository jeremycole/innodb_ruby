# frozen_string_literal: true

module Innodb
  class Sdi
    # A hash of page types to specialized classes to handle them. Normally
    # subclasses will register themselves in this list.
    @specialized_classes = {}

    class << self
      attr_reader :specialized_classes

      def register_specialization(id, specialized_class)
        @specialized_classes[id] = specialized_class
      end
    end

    attr_reader :space

    def initialize(space)
      @space = space
    end

    def sdi_header
      @sdi_header ||= space.page(0).sdi_header
    end

    def version
      sdi_header[:version]
    end

    def root_page_number
      sdi_header[:root_page_number]
    end

    def valid?
      root_page_number != 0
    end

    def index
      return unless valid?

      space.index(root_page_number)
    end

    def each_object
      return unless valid?
      return enum_for(:each_object) unless block_given?

      index.each_record do |record|
        yield SdiObject.from_record(record)
      end

      nil
    end

    def each_table
      return enum_for(:each_table) unless block_given?

      each_object { |o| yield o if o.is_a?(Table) }

      nil
    end

    def each_tablespace
      return enum_for(:each_tablespace) unless block_given?

      each_object { |o| yield o if o.is_a?(Tablespace) }

      nil
    end
  end
end
