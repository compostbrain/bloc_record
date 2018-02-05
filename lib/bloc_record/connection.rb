require 'sqlite3'
require 'pg'

module Connection
  def connection
    case BlocRecord.db_type
    when :sqlite
      @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
    when :pg
      @connection ||= PG::Connection.open(dbname: BlocRecord.database_filename)
    else
      raise ArgumentError "That is not a supported database type."
    end
  end

  def execute(sql:, params: nil)
    case BlocRecord.db_type
    when :sqlite
      params == nil ? connection.execute(sql) : connection.execute(sql, params)
    when :pg
      params == nil ? connection.exec(sql) : connection.exec_params(sql, params)
  end
end
