require 'vertica'

module Sequel
  module Vertica
    class Database < Sequel::Database
      ::Vertica::Connection.send(:alias_method,:execute, :query)
      PK_NAME = 'C_PRIMARY'
      set_adapter_scheme :vertica

      def connect(server)
        opts = server_opts(server)
        ::Vertica::Connection.new(
          :host => opts[:host],
          :user => opts[:user],
          :password => opts[:password],
          :port => opts[:port],
          :schema => opts[:schema],
          :database => opts[:database],
          :ssl => opts[:ssl] )
      end

      def execute(sql, opts = {}, &block)
        res = nil
        synchronize(opts[:server]) do |conn|
          res = log_yield(sql) { conn.query(sql) }
          res.each(&block)
        end
        res
      rescue ::Vertica::Error => e
        raise_error(e)
      end

      def execute_insert(sql, opts = {}, &block)
        result = execute(sql, opts, &block)
        result.first[:OUTPUT]
      end

      alias_method :execute_dui, :execute

      def supports_create_table_if_not_exists?
        true
      end

      def supports_drop_table_if_exists?
        true
      end

      def supports_transaction_isolation_levels?
        true
      end

      def identifier_input_method_default
        nil
      end

      def identifier_output_method_default
        nil
      end

      def locks
        dataset.from(:v_monitor__locks)
      end

      def tables(options = {} )
        schema = options[:schema]
        filter = {}
        filter[:table_schema] = schema.to_s if schema

        ds = dataset.select(:table_name).from(:v_catalog__tables).
          filter(filter)

        ds.to_a.map{ |h| h[:table_name].to_sym }
      end

      def schema_parse_table(table_name, options = {})
        schema = options[:schema]

        selector = [:column_name, :constraint_name, :is_nullable.as(:allow_null), 
                    (:column_default).as(:default), (:data_type).as(:db_type)]
        filter = { :table_name => table_name }
        filter[:table_schema] = schema.to_s if schema

        dataset = metadata_dataset.select(*selector).filter(filter).
          from(:v_catalog__columns).left_outer_join(:v_catalog__table_constraints, :table_id => :table_id)
        
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          row[:primary_key] = row.delete(:constraint_name) == PK_NAME
          [row.delete(:column_name).to_sym, row]
        end
      end

    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self
      EXPLAIN = 'EXPLAIN '
      EXPLAIN_LOCAL = 'EXPLAIN LOCAL '
      QUERY_PLAN = 'QUERY PLAN' 

      def columns
        return @columns if @columns
        ds = unfiltered.unordered.clone(:distinct => nil, :limit => 0, :offset=>nil)
        res = @db.execute(ds.select_sql)
        @columns = res.columns.map { |c| c.name }
        @columns
      end


      def fetch_rows(sql)
        execute(sql) do |row| 
          yield row 
        end
      end

      def explain(opts={})
        execute((opts[:local] ? EXPLAIN_LOCAL : EXPLAIN) + select_sql).map{ |k, v| k == QUERY_PLAN }.join("\$")
      end
    end
  end
end
