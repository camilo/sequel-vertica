require_relative '../shared/vertica'

module Sequel
  module ODBC
    Sequel.synchronize do
      Sequel::ODBC::DATABASE_SETUP[:vertica] = proc do |db|
        db.extend ::Sequel::ODBC::Vertica::DatabaseMethods
        db.extend_datasets ::Sequel::Vertica::DatasetMethods
      end
    end

    module Vertica
      module DatabaseMethods
        include Sequel::Vertica::DatabaseMethods
        
        # Return the last inserted identity value.
        def execute_insert(sql, opts=OPTS)
          synchronize(opts[:server]) do |conn|
            begin
              log_connection_yield(sql, conn){conn.do(sql)}
              begin
                last_insert_id_sql = 'SELECT LAST_INSERT_ID()'
                s = log_connection_yield(last_insert_id_sql, conn){conn.run(last_insert_id_sql)}
                if (rows = s.fetch_all) and (row = rows.first) and (v = row.first)
                  Integer(v)
                end
              ensure
                s.drop if s
              end
            rescue ::ODBC::Error => e
              raise_error(e)
            end
          end
        end
      end
    end
  end
end
