module BlocRecord
  def self.connect_to(filename:, db_type:)
    @database_filename = filename
    @db_type = db_type
  end

  def self.database_filename
    @database_filename
  end

  def self.db_type
    @db_type
  end
end
