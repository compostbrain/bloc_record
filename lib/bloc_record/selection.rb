require 'sqlite3'

module Selection
  def find(*ids)
    ids.each do |id|
      unless id.is_a?(Integer) && id > 0
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

  def where(*args)
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map {|key, value|  "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      end
    end

    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{expression};
    SQL

    rows = connection.execute(sql, params)
    rows_to_array(rows)
  end

  def order(*args)
    normalized_args = []

    args.each do |arg|
      normalize_order_arg(arg, normalized_args)
    end

    order = normalized_args.join(",")

    rows = connection.execute <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order};
    SQL

    rows_to_array(rows)
  end

  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
      rows = connection.execute <<-SQL
        SELECT * FROM #{table} #{joins}
      SQL
    else
      case args.first
      when String
        rows = connection.execute <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
        SQL
      when Symbol
        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
        SQL
      when Hash
        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first.key} ON #{args.first.key}.#{table}_id = #{table}.id
          INNER JOIN #{args.first.value} ON #{args.first.value}.#{args.first.key}_id = #{args.first.key}.id;
        SQL
      end
    end

    rows_to_array(rows)
  end


  private

  VALID_ORDER_MODIFIERS = [:asc, :desc, :ASC, :DESC,
                      "asc", "desc", "ASC", "DESC"]

  def validate_order_modifier(value)
    unless VALID_ORDER_MODIFIERS.include?(value)
      raise ArguementError, "Order modifier \"#{value}\" is invalid. Valid order modifiers include: #{VALID_ORDER_MODIFIERS.inspect}"
    end
  end

  def normalize_order_arg(arg, normalized_args)
    case arg
    when String
      if arg.include? ","
        args = arg.split(", ")
        args.each {|arg| normalize_order_arg(arg, normalized_args)}
      else
        conditions = arg.split(" ")
        key = conditions[0]
        value = conditions[1]
        if value == nil
          arg = key.to_sym
          normalize_order_arg(arg, normalized_args)
        else
          arg = {}
          arg[key] = value
          normalize_order_arg(arg, normalized_args)
        end
      end
    when Symbol
      normalized_args << arg.to_s
    when Hash
      value = arg.values.first
      key = arg.keys.first
      validate_order_modifier(value)
      normalized_args << "#{key} #{value.to_s.upcase}"
    else
      raise "That is not a supported type"
    end
    normalized_args
  end

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
