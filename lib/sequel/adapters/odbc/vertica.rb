require_relative '../shared/vertica'

Sequel.synchronize do
  Sequel::ODBC::DATABASE_SETUP[:vertica] = proc do |db|
    db.extend ::Sequel::Vertica::DatabaseMethods
    db.extend_datasets ::Sequel::Vertica::DatasetMethods
  end
end
