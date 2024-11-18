# frozen_string_literal: true

require "csv"

module Innodb
  class DataType
    class InvalidSpecificationError < StandardError; end

    # A hash of page types to specialized classes to handle them. Normally
    # subclasses will register themselves in this list.
    @specialized_classes = {}

    class << self
      attr_reader :specialized_classes
    end

    def self.register_specialization(data_type, specialized_class)
      @specialized_classes[data_type] = specialized_class
    end

    def self.specialization_for(data_type)
      # This needs to intentionally use Innodb::Page because we need to register
      # in the class instance variable in *that* class.
      Innodb::DataType.register_specialization(data_type, self)
    end

    def self.specialization_for?(data_type)
      Innodb::DataType.specialized_classes.include?(data_type)
    end

    def self.ceil_to(value, multiple)
      ((value + (multiple - 1)) / multiple) * multiple
    end

    module HasNumericModifiers
      def coerce_modifiers(modifiers)
        modifiers = modifiers&.split(",") if modifiers.is_a?(String)
        modifiers&.map(&:to_i)
      end
    end

    module HasStringListModifiers
      def coerce_modifiers(modifiers)
        CSV.parse_line(modifiers, quote_char: "'")&.map(&:to_s)
      end

      def formatted_modifiers
        CSV.generate_line(modifiers, quote_char: "'", force_quotes: true, row_sep: "")
      end
    end

    attr_reader :type_name
    attr_reader :modifiers
    attr_reader :properties

    def initialize(type_name, modifiers = nil, properties = nil)
      @type_name = type_name
      @modifiers = Array(coerce_modifiers(modifiers))
      @properties = Array(properties)
    end

    def variable?
      false
    end

    def blob?
      false
    end

    def value(data)
      data
    end

    def coerce_modifiers(modifiers)
      modifiers
    end

    def formatted_modifiers
      modifiers.join(",")
    end

    def format_type_name
      [
        [
          type_name.to_s,
          modifiers&.any? ? "(#{formatted_modifiers})" : nil,
        ].compact.join,
        *properties&.map { |p| p.to_s.sub("_", " ") },
      ].compact.join(" ")
    end

    def name
      @name ||= format_type_name
    end

    def length
      raise NotImplementedError
    end

    # Parse a data type definition and extract the base type and any modifiers.
    def self.parse_type_name_and_modifiers(type_string)
      matches = /^(?<type_name>[a-zA-Z0-9_]+)(?:\((?<modifiers>.+)\))?(?<properties>\s+unsigned)?$/.match(type_string)
      raise "Unparseable type #{type_string}" unless matches

      type_name = matches[:type_name].upcase.to_sym
      return [type_name, []] unless matches[:modifiers]

      # Use the CSV parser since it can understand quotes properly.
      [type_name, matches[:modifiers]]
    end

    def self.parse(type_string, properties = nil)
      type_name, modifiers = parse_type_name_and_modifiers(type_string.to_s)

      type_class = Innodb::DataType.specialized_classes[type_name]
      raise "Unrecognized type #{type_name}" unless type_class

      type_class.new(type_name, modifiers, properties)
    end
  end
end
