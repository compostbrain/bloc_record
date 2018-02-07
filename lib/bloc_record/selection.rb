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
      rows = execute <<-SQL
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
      rows = execute <<-SQL
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
    rows = execute <<-SQL
      SELECT #{columns.join ','} FROM #{table};
    SQL

    rows_to_array(rows)
  end

  def method_missing(m, *args)
    if m.to_s =~ /^find_by_(.*)$/ && columns.include?(Regexp.last_match(1))
      find_by(Regexp.last_match(1).to_sym, args.first)
    else

      raise ArgumentError, "#{Regexp.last_match(1)} is not an existing attribute"

    end
  end

  def find_each(start: 0, batch_size: 1000)
    rows = connection.excecute <<-SQL
      SELECT #{columns.join(',')} FROM #{table}
      LIMIT #{batch_size} OFFSET #{start};
    SQL

    if block_given?
      rows_to_array(rows).each { |object| yield object }
    else
      rows_to_array(rows)
    end
  end

  def find_in_batches(start: 0, batch_size: 1000)
    rows = get_batch_rows(columns, table, batch_size, start)
    if block_given?

      records = rows_to_array(rows)
      while records.any?
        records_size = records.size
        primary_key_offset = records.last[:id]

        yield records

        break if records_size < batch_size
        rows = get_batch_rows(columns, table, batch_size, primary_key_offset)
        records = rows_to_array(rows)
      end
    else
      rows_to_array(rows)
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
        expression = expression_hash.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }.join(' and ')
      end
    end

    sql = <<-SQL
      SELECT #{columns.join ','} FROM #{table}
      WHERE #{expression};
    SQL

    rows = execute(sql, params)
    rows_to_array(rows)
  end

  def order(*args)
    normalized_args = []

    args.each do |arg|
      normalize_order_arg(arg, normalized_args)
    end

    order = normalized_args.join(',')

    rows = execute <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order};
    SQL

    rows_to_array(rows)
  end

  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
      rows = execute <<-SQL
        SELECT * FROM #{table} #{joins}
      SQL
    else
      case args.first
      when String
        rows = execute <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
        SQL
      when Symbol
        rows = execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
        SQL
      when Hash
        rows = execute <<-SQL
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
                           'asc', 'desc', 'ASC', 'DESC'].freeze

  def get_batch_rows(columns, table, batch_size, offset)
    connection.execute <<-SQL
      SELECT #{columns.join(',')} FROM #{table}
      LIMIT #{batch_size} OFFSET #{offset};
    SQL
  end

  def validate_order_modifier(value)
    unless VALID_ORDER_MODIFIERS.include?(value)
      raise ArguementError, "Order modifier \"#{value}\" is invalid. Valid order modifiers include: #{VALID_ORDER_MODIFIERS.inspect}"
    end
  end

  def normalize_order_arg(arg, normalized_args)
    case arg
    when String
      if arg.include? ','
        args = arg.split(', ')
        args.each { |arg| normalize_order_arg(arg, normalized_args) }
      else
        conditions = arg.split(' ')
        key = conditions[0]
        value = conditions[1]
        if value.nil?
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
      raise 'That is not a supported type'
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
    collection = BlocRecord::Collection.new
    rows.each { |row| collection << new(Hash[columns.zip(row)]) }
    collection
  end
end
