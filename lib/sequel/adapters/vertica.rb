require_relative 'shared/vertica'
require 'vertica'

module Sequel
  extension :core_extensions
  module Vertica

    class Database < Sequel::Database
      include Sequel::Vertica::DatabaseMethods

      set_adapter_scheme :vertica
      ::Vertica::Connection.send(:alias_method, :execute, :query)

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
      include Sequel::Vertica::DatasetMethods
      Database::DatasetClass = self

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
    end
  end
end
