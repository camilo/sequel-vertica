require 'spec_helper'

unless defined?(VERTICA_DB)
  VERTICA_URL = 'vertica://vertica:vertica@localhost:5432/reality_spec' unless defined? VERTICA_URL
  VERTICA_DB = Sequel.connect(ENV['SEQUEL_VERTICA_SPEC_DB']||VERTICA_URL)
end
INTEGRATION_DB = VERTICA_DB unless defined?(INTEGRATION_DB)

def VERTICA_DB.sqls
  (@sqls ||= [])
end


VERTICA_DB.create_table! :test do
  varchar :name
  integer :value
end
VERTICA_DB.create_table! :test2 do
  varchar :name
  integer :value
end
VERTICA_DB.create_table! :test3 do
  integer :value
  timestamp :time
end
VERTICA_DB.create_table! :test4 do
  varchar :name, :size => 20
  bytea :value
end

describe "A vertica sequel connection" do
  specify "sets a read timeout" do
    conn = Sequel.connect("#{ENV['SEQUEL_VERTICA_SPEC_DB']||VERTICA_URL}?read_timeout=1000")
    conn.synchronize do |raw_conn|
      expect(raw_conn.options[:read_timeout]).to eq(1000)
    end
  end
end

describe "A Vertica database" do

  before do
    @db = VERTICA_DB
  end

  specify "correctly parses the schema" do
    expect(@db.schema(:test3, :reload=>true)).to eq([
      [:value, {:type=>:integer, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"int", :primary_key=>false}],
      [:time, {:type=>:datetime, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"timestamp", :primary_key=>false}]
    ])
    expect(@db.schema(:test4, :reload=>true)).to eq([
      [:name, {:allow_null=>true, :default=>nil, :db_type=>"varchar(20)", :type=>:string, :primary_key=>false, :ruby_default=>nil, :max_length=>20}],
      [:value, {:allow_null=>true, :default=>nil, :db_type=>"varbinary(80)", :type=>:blob, :primary_key=>false, :ruby_default=>nil}]
    ])
  end

  specify "creates an auto incrementing primary key" do
    @db.create_table! :auto_inc_test do
      primary_key :id
      integer :value
    end
    expect(@db[<<-SQL].first[:COUNT]).to eq(1)
      SELECT COUNT(1) FROM v_catalog.sequences WHERE identity_table_name='auto_inc_test'
    SQL
  end

end

describe "A vertica dataset" do
  before do
    @d = VERTICA_DB[:test]
    @d.delete if @d.count > 0 # Vertica will throw an error if the table has just been created and does not have a super projection yet.
  end

  specify "quotes columns and tables using double quotes if quoting identifiers" do
    expect(@d.select(:name).sql).to eq( \
      'SELECT "name" FROM "test"'
    )

    expect(@d.select(Sequel.lit('COUNT(*)')).sql).to eq( \
      'SELECT COUNT(*) FROM "test"'
    )

    expect(@d.select(:max.sql_function(:value)).sql).to eq( \
      'SELECT max("value") FROM "test"'
    )

    expect(@d.select(:NOW.sql_function).sql).to eq( \
    'SELECT NOW() FROM "test"'
    )

    expect(@d.select(:max.sql_function(:items__value)).sql).to eq( \
      'SELECT max("items"."value") FROM "test"'
    )

    expect(@d.order(:name.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC'
    )

    expect(@d.select(Sequel.lit('test.name AS item_name')).sql).to eq( \
      'SELECT test.name AS item_name FROM "test"'
    )

    expect(@d.select(Sequel.lit('"name"')).sql).to eq( \
      'SELECT "name" FROM "test"'
    )

    expect(@d.select(Sequel.lit('max(test."name") AS "max_name"')).sql).to eq( \
      'SELECT max(test."name") AS "max_name" FROM "test"'
    )

    expect(@d.insert_sql(:x => :y)).to match( \
      /\AINSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?\z/
    )

  end

  specify "quotes fields correctly when reversing the order if quoting identifiers" do
    expect(@d.reverse_order(:name).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC'
    )

    expect(@d.reverse_order(:name.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" ASC'
    )

    expect(@d.reverse_order(:name, :test.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'
    )

    expect(@d.reverse_order(:name.desc, :test).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
    )
  end

  specify "supports regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}

    expect(@d.filter(:name => /bc/).count).to eq(2)
    expect(@d.filter(:name => /^bc/).count).to eq(1)
  end

  specify "should support ilike operator" do
    expect(@d.where(Sequel.ilike(:name, '%acme%')).sql).to eq(%{SELECT * FROM "test" WHERE ("name" ILIKE '%acme%' ESCAPE '\\')})
    expect(@d.where(~Sequel.ilike(:name, '%acme%')).sql).to eq(%{SELECT * FROM "test" WHERE ("name" NOT ILIKE '%acme%' ESCAPE '\\')})
  end

  specify "supports case-insensitive regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}

    expect(@d.filter(:name => /BC/i).count).to eq(2)
    expect(@d.filter(:name => /^BC/i).count).to eq(1)
  end

  specify "#columns returns the correct column names" do
    expect(@d.columns!).to eq([:name, :value])
    expect(@d.select(:name).columns!).to eq([:name])
  end
end

describe "A Vertica dataset with a timestamp field" do
  before do
    @db = VERTICA_DB
    @d = @db[:test3]
    @d.delete if @d.count > 0 # Vertica will throw an error if the table has just been created and does not have a super projection yet.
  end
  after do
    @db.convert_infinite_timestamps = false if @db.adapter_scheme == :postgres
  end

  cspecify "stores milliseconds in time fields for Time objects", :do, :swift do
    t = Time.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>1][:time]
    expect(@d.literal(t2)).to eq(@d.literal(t))
    expect(t2.strftime('%Y-%m-%d %H:%M:%S')).to eq(t.strftime('%Y-%m-%d %H:%M:%S'))
    expect(t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).to eq(t.usec)
  end

  cspecify "stores milliseconds in time fields for DateTime objects", :do, :swift do
    t = DateTime.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>1][:time]
    expect(@d.literal(t2)).to eq(@d.literal(t))
    expect(t2.strftime('%Y-%m-%d %H:%M:%S')).to eq(t.strftime('%Y-%m-%d %H:%M:%S'))
    expect(t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).to eq(t.strftime('%N').to_i/1000)
  end

  describe "Verticas's EXPLAIN and EXPLAIN LOCAL" do
    specify "should not raise errors" do
      @d = VERTICA_DB[:test3]
      expect{@d.explain}.not_to raise_error
      expect{@d.explain(:local => true)}.not_to raise_error
    end
  end

  describe "Vertica's TIMESERIES clause" do
    let(:timeseries_opts) {
      {
        alias: :slice_time,
        time_unit: '1 second',
        over: { order: :occurred_at }
      }
    }
    [:alias, :time_unit, :over].each do |option|
      specify "requires #{option}" do
        expect { @d.timeseries(timeseries_opts.reject { |k,v| k == option }) }.to \
          raise_error(ArgumentError, /timeseries requires.*#{option}/)
      end
    end

    specify "it renders to SQL correctly" do
      expect(@d.timeseries(timeseries_opts).sql).to eq(\
        %(SELECT * FROM "test3" TIMESERIES slice_time AS '1 second' OVER (ORDER BY "occurred_at"))
      )
    end
  end
end

describe "A Vertica database" do
  before do
    @db = VERTICA_DB
  end

  specify "supports ALTER TABLE DROP COLUMN" do
    @db.create_table!(:test3) { varchar :name; integer :value }
    expect(@db[:test3].columns).to eq([:name, :value])
    @db.drop_column :test3, :value
    expect(@db[:test3].columns).to eq([:name])
  end

  specify "It does not support ALTER TABLE ALTER COLUMN TYPE" do
    @db.create_table!(:test4) { varchar :name; integer :value }
    expect{ @db.set_column_type :test4, :value, :float }.to raise_error(Sequel::DatabaseError,
                                                    /Syntax error at or near "TYPE"/)
  end

  specify "supports rename column operations" do
    @db.create_table!(:test5) { varchar :name; integer :value }
    @db[:test5] << {:name => 'mmm', :value => 111}
    @db.rename_column :test5, :value, :val
    expect(@db[:test5].columns).to eq([:name, :val])
    expect(@db[:test5].first[:val]).to eq(111)
  end

  specify "supports add column operations" do
    @db.create_table!(:test2) { varchar :name; integer :value }
    expect(@db[:test2].columns).to eq([:name, :value])

    @db.add_column :test2, :xyz, :varchar, :default => '000'
    expect(@db[:test2].columns).to eq([:name, :value, :xyz])
    @db[:test2] << {:name => 'mmm', :value => 111}
    expect(@db[:test2].first[:xyz]).to eq('000')
  end

  specify "#locks should be a dataset returning database locks " do
    expect(@db.locks).to be_a_kind_of(Sequel::Dataset)
    expect(@db.locks.all).to be_a_kind_of(Array)
  end
end

describe "Vertica::Database#copy_into" do
  before do
    @db = VERTICA_DB
    @db[:test].truncate
  end

  specify "takes data in an enumerable passed in with :data" do
    strings = ["firstname|1"]
    @db.copy_into(:test, data: strings)
    expect(@db[:test].all.first.to_h).to eq({name: "firstname", value: 1})
  end

  specify "takes data from a block" do
    strings = ["firstname|1"]
    @db.copy_into(:test) {
      strings.pop
    }
    expect(@db[:test].all.first.to_h).to eq({name: "firstname", value: 1})
  end

  specify "errors if both a block and :data are specified" do
    expect { @db.copy_into(:test, data: ["a string"]) { "a block" } }.to \
      raise_error(ArgumentError, "Cannot provide both a :data option and a block to copy_into")
  end

  specify "errors if neither block nor :data are specified" do
    expect { @db.copy_into(:test) }.to \
      raise_error(ArgumentError, "Must provide either a :data option or a block to copy_into")
  end

  specify "allows data columns to be specified" do
    @db.copy_into(:test, columns: [:value, :name], data: ["1|firstname"])
    expect(@db[:test].all.first.to_h).to eq({name: "firstname", value: 1})
  end

  specify "allows an options string to be appended" do
    @db.copy_into(:test, data: ["lastname,2"], options: "DELIMITER ','")
    expect(@db[:test].all.first.to_h).to eq({name: "lastname", value: 2})
  end

  specify "converts format: :csv to the correct SQL option" do
    @db.copy_into(:test, data: ["lastname,2"], format: :csv)
    expect(@db[:test].all.first.to_h).to eq({name: "lastname", value: 2})
  end
end

describe "Vertica::Dataset#insert" do
  before do
    @db = VERTICA_DB
    @db.create_table!(:test5){ :xid; Integer :value}
    @db.sqls.clear
    @ds = @db[:test5]
  end

  after do
    @db.drop_table?(:test5)
  end

  specify "works with static SQL" do
    expect(@ds.with_sql('INSERT INTO test5 (value) VALUES (10)').insert).to eq(1)
    expect(@db['INSERT INTO test5 (value) VALUES (20)'].insert).to eq(1)
    expect(@ds.all).to include({:value=>10}, {:value=>20})
  end

  specify "inserts correctly if using a column array and a value array" do
    expect(@ds.insert([:value], [10])).to eq(1)
    expect(@ds.all).to eq([{:value=>10}])
  end
end

describe "Vertica::Database schema qualified tables" do
  before do
    VERTICA_DB << "CREATE SCHEMA schema_test"
    VERTICA_DB.instance_variable_set(:@primary_keys, {})
    VERTICA_DB.instance_variable_set(:@primary_key_sequences, {})
  end

  after do
    VERTICA_DB << "DROP SCHEMA schema_test CASCADE"
  end

  specify "should be able to create, drop, select and insert into tables in a given schema" do
    VERTICA_DB.create_table(:schema_test__table_in_schema_test){integer :i}
    expect(VERTICA_DB[:schema_test__table_in_schema_test].first).to eq(nil)
    expect(VERTICA_DB[:schema_test__table_in_schema_test].insert(:i=>1)).to eq(1)
    expect(VERTICA_DB[:schema_test__table_in_schema_test].first).to eq({:i=>1})
    expect(VERTICA_DB.from(Sequel.lit('schema_test.table_in_schema_test')).first).to eq({:i=>1})
    VERTICA_DB.drop_table(:schema_test__table_in_schema_test)
    VERTICA_DB.create_table(:table_in_schema_test.qualify(:schema_test)){integer :i}
    expect(VERTICA_DB[:schema_test__table_in_schema_test].first).to eq(nil)
    expect(VERTICA_DB.from(Sequel.lit('schema_test.table_in_schema_test')).first).to eq(nil)
    VERTICA_DB.drop_table(:table_in_schema_test.qualify(:schema_test))
  end

  specify "#tables should not include tables in a default non-public schema" do
    VERTICA_DB.create_table(:schema_test__table_in_schema_test){integer :i}
    expect(VERTICA_DB.tables).to include(:table_in_schema_test)
    expect(VERTICA_DB.tables).not_to include(:tables)
    expect(VERTICA_DB.tables).not_to include(:columns)
    expect(VERTICA_DB.tables).not_to include(:locks)
    expect(VERTICA_DB.tables).not_to include(:domain_udt_usage)
  end

  specify "#tables should return tables in the schema provided by the :schema argument" do
    VERTICA_DB.create_table(:schema_test__table_in_schema_test){integer :i}
    expect(VERTICA_DB.tables(:schema=>:schema_test)).to eq([:table_in_schema_test])
  end

  specify "#schema should not include columns from tables in a default non-public schema" do
    VERTICA_DB.create_table(:schema_test__domains){integer :i}
    sch = VERTICA_DB.schema(:domains)
    cs = sch.map{|x| x.first}
    expect(cs).to include(:i)
    expect(cs).not_to include(:data_type)
  end

  specify "#schema should only include columns from the table in the given :schema argument" do
    VERTICA_DB.create_table!(:domains){integer :d}
    VERTICA_DB.create_table(:schema_test__domains){integer :i}
    sch = VERTICA_DB.schema(:domains, :schema=>:schema_test)
    cs = sch.map{|x| x.first}
    expect(cs).to include(:i)
    expect(cs).not_to include(:d)
    VERTICA_DB.drop_table(:domains)
  end

  specify "#table_exists? should see if the table is in a given schema" do
    VERTICA_DB.create_table(:schema_test__schema_test){integer :i}
    expect(VERTICA_DB.table_exists?(:schema_test__schema_test)).to eq(true)
  end

end
