require 'sqlite3'

module Selection
  def find(*ids)
    ids.each do |id|
      unless id.is_a? Integer && id > 0
        raise ArgumentError, 'ID must be a postive integer'
      end
    end
    if ids.length == 1
      find_one(ids.first)
    else
      rows = connection.execute <<-SQL
        SELECT #{columns.join ','} FROM #{table}
        WHERE id IN (#{ids.join(',')});
      SQL

      rows_to_array(rows)
    end
  end

  def find_one(id)
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      WHERE id = #{id};
    SQL

    init_object_from_row(row)
  end

  def find_by(attribute, value)
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    init_object_from_row(row)
  end

  def take(num = 1)
    if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ','} FROM #{table}
        ORDER BY random()
        LIMIT #{num};
      SQL

      rows_to_array(rows)
    else
      take_one
    end
  end

  def take_one
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def first
    puts BlocRecord::Utility.underscore(table)
    puts connection.get_first_row <<-SQL
    SELECT #{columns.join ','} FROM #{table}
    ORDER BY id ASC LIMIT 1;
  SQL
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ','} FROM #{table};
    SQL

    rows_to_array(rows)
  end

  def method_missing(m, *args, &block)
    if m.to_s =~ /^find_by_(.*)$/ && columns.include?($1)
      find_by($1.to_sym, args.first)
    else
      raise ArgumentError, "#{$1} is not an existing attribute"
    end
  end

  def find_each(start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil)
       if block_given?
         find_in_batches(start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore) do |records|
           records.each { |record| yield record }
         end
       else
         enum_for(:find_each, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore) do
           relation = self
           apply_limits(relation, start, finish).size
         end
       end
     end

  def find_in_batches(start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil)
      relation = self
      unless block_given?
        return to_enum(:find_in_batches, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore) do
          total = apply_limits(relation, start, finish).size
          (total - 1).div(batch_size) + 1
        end
      end

      in_batches(of: batch_size, start: start, finish: finish, load: true, error_on_ignore: error_on_ignore) do |batch|
        yield batch.to_a
      end
    end

  private

  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end

  def rows_to_array(rows)
    rows.map { |row| new(Hash[columns.zip(row)]) }
  end
end
