# frozen_string_literal: true

require "forwardable"

module Innodb
  class DataDictionary
    class ObjectStore
      extend Forwardable
      def_delegators :@objects, :[], :first, :each, :empty?, :any?, :count

      class ObjectTypeError < RuntimeError; end

      attr_reader :allowed_type
      attr_reader :objects

      def initialize(allowed_type: Object)
        @allowed_type = allowed_type
        @objects = []
      end

      def add(new_object)
        raise ObjectTypeError unless new_object.is_a?(@allowed_type)

        @objects.push(new_object)

        new_object
      end

      def make(**attributes)
        add(@allowed_type.new(**attributes))
      end

      def by(**attributes)
        @objects.select { |o| attributes.all? { |k, v| o.send(k) == v } }
      end

      def find(**attributes)
        by(**attributes).first
      end
    end
  end
end
