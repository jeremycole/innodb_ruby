# -*- encoding : utf-8 -*-

#
# A class to describe record layouts for InnoDB indexes. Designed to be usable
# in two different ways: statically built and dynamically built. Note that in
# both cases, the order that statements are encountered is critical. Columns
# must be added to the key and row structures in the correct order.
#
# STATIC USAGE
#
# Static building is useful for building a custom describer for any index
# and looks like the following:
#
# To describe the SQL syntax:
#
#   CREATE TABLE my_table (
#     id BIGINT NOT NULL,
#     name VARCHAR(100) NOT NULL,
#     age INT UNSIGNED,
#     PRIMARY KEY (id)
#   );
#
# The clustered key would require a class like:
#
#   class MyTableClusteredDescriber < Innodb::RecordDescriber
#     type :clustered
#     key "id", :BIGINT, :UNSIGNED, :NOT_NULL
#     row "name", "VARCHAR(100)", :NOT_NULL
#     row "age", :INT, :UNSIGNED
#   end
#
# It can then be instantiated as usual:
#
#   my_table_clustered = MyTableClusteredDescriber.new
#
# All statically-defined type, key, and row information will be copied into
# the instance when it is initialized. Once initialized, the instance can
# be additionally used dynamically, as per below. (A dynamic class is just
# the same as a static class that is empty.)
#
# Note that since InnoDB works in terms of *indexes* individually, a new class
# must be created for each index.
#
# DYNAMIC USAGE
#
# If a record describer needs to be built based on runtime information, such
# as index descriptions from a live data dictionary, instances can be built
# dynamically. For the same table above, this would require:
#
#   my_table_clustered = Innodb::RecordDescriber.new
#   my_table_clustered.type = :clustered
#   my_table_clustered.key "id", :BIGINT, :UNSIGNED, :NOT_NULL
#   my_table_clustered.row "name", "VARCHAR(100)", :NOT_NULL
#   my_table_clustered.row "age", :INT, :UNSIGNED
#

class Innodb::RecordDescriber
  # Internal method to initialize the class's instance variable on access.
  def self.static_description
    @static_description ||= {
      :type => nil,
      :key => [],
      :row => []
    }
  end

  # A 'type' method to be used from the DSL.
  def self.type(type)
    static_description[:type] = type
  end

  # An internal method wrapped with 'key' and 'row' helpers.
  def self.add_static_field(group, name, type)
    static_description[group] << { :name => name, :type => type }
  end

  # A 'key' method to be used from the DSL.
  def self.key(name, *type)
    add_static_field :key, name, type
  end

  # A 'row' method to be used from the DSL.
  def self.row(name, *type)
    add_static_field :row, name, type
  end

  attr_accessor :description

  def initialize
    @description = self.class.static_description.dup
    @description[:key] = @description[:key].dup
    @description[:row] = @description[:row].dup
  end

  # Set the type of this record (:clustered or :secondary).
  def type(type)
    description[:type] = type
  end

  # An internal method wrapped with 'key' and 'row' helpers.
  def add_field(group, name, type)
    description[group] << { :name => name, :type => type }
  end

  # Add a key column to the record description.
  def key(name, *type)
    add_field :key, name, type
  end

  # Add a row (non-key) column to the record description.
  def row(name, *type)
    add_field :row, name, type
  end

  def field_names
    names = []
    [:key, :row].each do |group|
      names += description[group].map { |n| n[:name] }
    end
    names
  end

  def generate_class(name="Describer_#{object_id}")
    str = "class #{name}\n"
    str << "  type %s\n" % [
      description[:type].inspect
    ]
    [:key, :row].each do |group|
      description[group].each do |item|
        str << "  %s %s, %s\n" % [
          group,
          item[:name].inspect,
          item[:type].map { |s| s.inspect }.join(", "),
        ]
      end
    end
    str << "end\n"
    str
  end
end
