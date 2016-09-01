require 'vertica'

module Sequel
  extension :core_extensions
  module Vertica

    class CreateTableGenerator < Sequel::Schema::CreateTableGenerator
      def primary_key(name, *args)
        super

        if @primary_key[:auto_increment]
          @primary_key.delete(:auto_increment)
          @primary_key[:type] = Vertica::Database::AUTO_INCREMENT
        end

        @primary_key
      end
    end

    class Database < Sequel::Database

      ::Vertica::Connection.send(:alias_method, :execute, :query)

      PK_NAME = 'C_PRIMARY'
      AUTO_INCREMENT = 'AUTO_INCREMENT'
      set_adapter_scheme :vertica

      def connect(server)
        opts = server_opts(server)
        ::Vertica::Connection.new(
          :host => opts[:host],
          :user => opts[:user],
          :password => opts[:password],
          :port => opts[:port],
          :database => opts[:database],
          :read_timeout => opts[:read_timeout].nil? ? nil : opts[:read_timeout].to_i,
          :ssl => opts[:ssl]
        )
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

      # +copy_into+ uses Vertica's +COPY FROM STDIN+ SQL statement to do very fast inserts
      # into a table using any formatting options supported by Vertica.
      # This method is only supported if vertica 1.0.0+ is the underlying ruby driver.
      # This method should only be called if you want
      # results returned to the client.  If you are using +COPY FROM+
      # with a filename, you should just use +run+ instead of this method.
      #
      # The following options are respected:
      #
      # :columns :: The columns to insert into, with the same order as the columns in the
      #             input data.  If this isn't given, uses all columns in the table.
      # :data :: The data to copy to Vertica, which should already be in pipe-separated or CSV
      #          format.  This can be either a string, or any object that responds to
      #          each and yields string.
      # :format :: Vertica does not support FORMAT on the data (instead, it supports FORMAT on
      #            the individual columns). However, for postgresql compatibility, if this
      #            option is set to :csv, then " DELIMITER ','" will be appended to the
      #            :options string below. If :options is not specified, it will be set to
      #            " DELIMITER ','".
      # :options :: An options SQL string to use, which should contain space-separated options.
      #
      # If a block is provided and :data option is not, this will yield to the block repeatedly.
      # The block should return a string, or nil to signal that it is finished.
      def copy_into(table, opts=OPTS)
        data = opts[:data]
        if opts[:format] == :csv
          opts[:options] ||= ""
          opts[:options] += " DELIMITER ','"
        end
        data = Array(data) if data.is_a?(String)

        if block_given? && data
          raise ArgumentError, "Cannot provide both a :data option and a block to copy_into"
        elsif !block_given? && !data
          raise ArgumentError, "Must provide either a :data option or a block to copy_into"
        end

        synchronize(opts[:server]) do |conn|
          conn.copy(copy_into_sql(table, opts)) do |io|
            begin
              if block_given?
                while buf = yield
                  io.write(buf.chomp + "\n")
                end
              else
                data.each { |buff|
                  io.write(buff.chomp + "\n")
                }
              end
            end
          end
        end
      end

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

      def auto_increment_sql
        AUTO_INCREMENT
      end

      def create_table_generator_class
        Vertica::CreateTableGenerator
      end

      def tables(options = {})
        schema = options[:schema]
        filter = {}
        filter[:table_schema] = schema.to_s if schema

        dataset.select(:table_name).
          from(:v_catalog__tables).
          filter(filter).
          to_a.
          map { |h| h[:table_name].to_sym }
      end

      def schema_parse_table(table_name, options = {})
        schema = options[:schema]

        selector = [:column_name, :constraint_name, :is_nullable.as(:allow_null),
                    (:column_default).as(:default), (:data_type).as(:db_type)]
        filter = { :columns__table_name => table_name.to_s }
        filter[:columns__table_schema] = schema.to_s if schema

        dataset = metadata_dataset.
          select(*selector).
          filter(filter).
          from(:v_catalog__columns).
          left_outer_join(:v_catalog__table_constraints, :table_id => :table_id)

        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          row[:primary_key] = row.delete(:constraint_name) == PK_NAME
          [row.delete(:column_name).to_sym, row]
        end
      end

      # SQL for doing fast table insert from stdin.
      def copy_into_sql(table, opts)
        sql = "COPY #{literal(table)} "
        if cols = opts[:columns]
          sql << literal(Array(cols))
        end
        sql << " FROM STDIN"
        if opts[:options]
          sql << " #{opts[:options]}" if opts[:options]
        end
        sql
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self
      EXPLAIN = 'EXPLAIN '.freeze
      EXPLAIN_LOCAL = 'EXPLAIN LOCAL '.freeze
      QUERY_PLAN = 'QUERY PLAN'.freeze
      TIMESERIES = ' TIMESERIES '.freeze
      OVER = ' OVER '.freeze
      AS = ' AS '.freeze
      REGEXP_LIKE = 'REGEXP_LIKE'.freeze
      SPACE = Dataset::SPACE
      PAREN_OPEN = Dataset::PAREN_OPEN
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      ESCAPE = Dataset::ESCAPE
      BACKSLASH = Dataset::BACKSLASH

      Dataset.def_sql_method(self, :select, %w(with select distinct columns from join timeseries where group having compounds order limit lock))

      def timeseries(opts={})
        raise ArgumentError, "timeseries requires :alias" unless opts[:alias]
        raise ArgumentError, "timeseries requires :time_unit" unless opts[:time_unit]
        raise ArgumentError, "timeseries requires an :over clause" unless opts[:over]

        clone(timeseries: {
                alias: opts[:alias],
                time_unit: opts[:time_unit],
                over: Sequel::SQL::Window.new(opts[:over])
              })
      end

      def select_timeseries_sql(sql)
        if ts_opts = opts[:timeseries]
          sql << TIMESERIES << ts_opts[:alias].to_s << AS << "'#{ts_opts[:time_unit]}'" << OVER
          window_sql_append(sql, ts_opts[:over].opts)
        end
      end

      def columns
        return @columns if @columns
        ds = unfiltered.unordered.clone(:distinct => nil, :limit => 0, :offset => nil)
        res = @db.execute(ds.select_sql)
        @columns = res.columns.map { |c| c.name.to_sym }
        @columns
      end

      def fetch_rows(sql)
        execute(sql) do |row|
          yield row.to_h.inject({}) { |a, (k,v)| a[k.to_sym] = v; a }
        end
      end

      def explain(opts={})
        execute((opts[:local] ? EXPLAIN_LOCAL : EXPLAIN) + select_sql).map { |k, v| k == QUERY_PLAN }.join("\$")
      end

      def supports_regexp?
        true
      end

      def supports_window_functions?
        true
      end

      def regexp_like(sql, source, pattern, options = nil)
        sql << REGEXP_LIKE
        sql << PAREN_OPEN
        literal_append(sql, source)
        sql << COMMA
        literal_append(sql, pattern)

        if options
          sql << COMMA
          literal_append(sql, options)
        end

        sql << PAREN_CLOSE
      end

      def complex_expression_sql_append(sql, op, args)
        case op
        when :ILIKE, :'NOT ILIKE'
          sql << PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << SPACE << op.to_s << SPACE
          literal_append(sql, args.at(1))
          sql << ESCAPE
          literal_append(sql, BACKSLASH)
          sql << PAREN_CLOSE
        when :'~'
          regexp_like(sql, args[0], args[1])
        when :'~*'
          regexp_like(sql, args[0], args[1], 'i')
        else
          super
        end
      end
    end
  end
end
