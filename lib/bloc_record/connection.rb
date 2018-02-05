require 'sqlite3'
require 'pg'

module Connection
  def connection
    db_type = BlocRecord.db_type
    case db_type
    when "sqlite"
      @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
    when "pg"
      @connection ||= PG::Connection.new(dbname: BlocRecord.database_filename)
    else
      raise ArgumentError "That is not a supported database type."
    end
  end
end
