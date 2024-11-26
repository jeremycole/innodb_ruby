# frozen_string_literal: true

module Innodb
  class MysqlCollation
    class DuplicateIdError < StandardError; end
    class DuplicateNameError < StandardError; end

    @collations = []
    @collations_by_id = {}
    @collations_by_name = {}

    class << self
      attr_reader :collations
    end

    def self.add(kwargs)
      raise DuplicateIdError if @collations_by_id.key?(kwargs[:id])
      raise DuplicateNameError if @collations_by_name.key?(kwargs[:name])

      collation = new(**kwargs)
      @collations.push(collation)
      @collations_by_id[collation.id] = collation
      @collations_by_name[collation.name] = collation
      @all_fixed_ids = nil
      collation
    end

    def self.by_id(id)
      @collations_by_id[id]
    end

    def self.by_name(name)
      @collations_by_name[name]
    end

    def self.all_fixed_ids
      @all_fixed_ids ||= Innodb::MysqlCollation.collations.select(&:fixed?).map(&:id).sort
    end

    attr_reader :id
    attr_reader :name
    attr_reader :character_set_name
    attr_reader :mbminlen
    attr_reader :mbmaxlen

    def initialize(id:, name:, character_set_name:, mbminlen:, mbmaxlen:)
      @id = id
      @name = name
      @character_set_name = character_set_name
      @mbminlen = mbminlen
      @mbmaxlen = mbmaxlen
    end

    def fixed?
      mbminlen == mbmaxlen
    end

    def variable?
      !fixed?
    end
  end
end
