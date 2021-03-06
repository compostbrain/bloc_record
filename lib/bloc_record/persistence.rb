require 'sqlite3'
require 'bloc_record/schema'
require 'pry'

module Persistence

  def self.included(base)
    base.extend(ClassMethods)
  end

  def save
    self.save! rescue false
  end

  def save!
    unless self.id
      self.id = self.class.create(BlocRecord::Utility.instance_variables_to_hash(self)).id
      BlocRecord::Utility.reload_obj(self)
      return true
    end

    fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(self.instance_variable_get("@#{col}"))}" }.join(",")

    self.class.execute <<-SQL
      UPDATE #{self.class.table}
      SET #{fields}
      WHERE id = #{self.id};
    SQL

    true
  end

  def update_attribute(attribute, value)
    self.class.update(self.id, { attribute => value })
  end

  def update_attributes(updates)

    return if updates.empty?

    self.class.update(self.id, updates)
  end

  def destroy
    self.class.destroy(self.id)
  end

  def method_missing(m, *args, &block)
    if m.to_s =~ /^update_(.*)$/ && columns.include?($1)
      self.class.update(self.id, { $1.to_sym => args.first })
    elsif m.to_s = /^to_ary(.*)$/
      return

    else
      puts m
      raise ArgumentError, "#{$1} is not an existing attribute"
    end
  end

  module ClassMethods

    def update_all(updates)
      update(nil, updates)
    end

    def destroy(*id)
      if id.length > 1
        where_clause = "WHERE id IN (#{id.join(",")});"
      else
        where_clause = "WHERE id = #{id.first};"
      end
      execute <<-SQL
        DELETE FROM #{table} #{where_clause}
      SQL

      true
    end

    def destroy_all(conditions = nil)
      conditions_hash = {}
      conditions_hash = conditions if Hash === conditions
      unless conditions == nil || Hash === conditions
        normalize_destroy_all_inputs(conditions, conditions_hash)
      end

      if conditions_hash && !conditions_hash.empty?
        conditions_hash = BlocRecord::Utility.convert_keys(conditions_hash)
        conditions = conditions_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")

        execute <<-SQL
          DELETE FROM #{table}
          WHERE #{conditions};
        SQL
      else
        execute <<-SQL
          DELETE FROM #{table}
        SQL
      end

      true
    end

    def normalize_destroy_all_inputs(conditions, conditions_hash)
      case conditions
      when Array # assumes key at even indexes and value at odd indexes
        conditions.each_with_index do |input, index|
          next if index % 2 == 1
          input_minus_whitespace = input.gsub(/\s+/, "")
          key = input_minus_whitespace[/^(.*)\=/, 1]
          value = conditions[index + 1]
          conditions_hash[key.to_sym] = value
        end
      when String # assumes string in format "phone_number = '999-999-9999'"
        conditions_minus_whitespace = conditions.gsub(/\s+/, "")
        key = conditions_minus_whitespace[/^(.*)\=/, 1]
        value = conditions_minus_whitespace[/\=(.*)/, 1]
        conditions_hash[key.to_sym] = value
      end
      conditions_hash
    end

    def create(attrs)
      attrs = BlocRecord::Utility.convert_keys(attrs)
      attrs.delete "id"
      vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }

      execute <<-SQL
        INSERT INTO #{table} (#{attributes.join ","})
        VALUES (#{vals.join ","});
      SQL

      data = Hash[attributes.zip attrs.values]
      data["id"] = execute("SELECT last_insert_rowid();")[0][0]
      new(data)
    end

    def update(id, updates)
      # assumes that if id is an array, updates is an array of hashes corresponding to ids to be updated
      case id
      when Array
        id.each_with_index do |id, index|
            update(id, updates[index])
        end
      else
        updates = BlocRecord::Utility.convert_keys(updates)
        updates.delete "id"

        updates_array = updates.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }

        execute <<-SQL
          UPDATE #{table}
          SET #{updates_array * ","}
          WHERE id = #{id};
        SQL

        true
      end
    end


  end
end
