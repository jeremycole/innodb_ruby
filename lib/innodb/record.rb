# -*- encoding : utf-8 -*-

class Innodb::Record
  attr_accessor :record

  def initialize(record)
    @record = record
  end

  def header
    record[:header]
  end

  def offset
    record[:offset]
  end

  def next
    record[:next]
  end

  def key
    record[:key]
  end

  def key_string
    key && key.map { |r| "%s=%s" % [r[:name], r[:value]] }.join(", ")
  end

  def row
    record[:row]
  end

  def row_string
    key && key.map { |r| "%s=%s" % [r[:name], r[:value]] }.join(", ")
  end

  def child_page_number
    record[:child_page_number]
  end

  def uncached_fields
    fields_hash = {}
    [:key, :row].each do |group|
      if record[group]
        record[group].each do |column|
          fields_hash[column[:name]] = column[:value]
        end
      end
    end
    fields_hash
  end

  def fields
    @fields ||= uncached_fields
  end
end
