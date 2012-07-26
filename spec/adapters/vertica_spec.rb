require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

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

describe "A Vertica database" do 

  before do
    @db = VERTICA_DB
  end

  specify "should correctly parse the schema" do
    @db.schema(:test3, :reload=>true).should == [
      [:value, {:type=>:integer, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"int", :primary_key=>false}],
      [:time, {:type=>:datetime, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"timestamp", :primary_key=>false}]
    ]
    @db.schema(:test4, :reload=>true).should == [
      [:name, {:type=>:string, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"varchar(20)", :primary_key=>false}],
      [:value, {:type=>:blob, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"varbinary(80)", :primary_key=>false}]
    ]
  end

end

describe "A vertica dataset" do
  before do
    @d = VERTICA_DB[:test]
    @d.delete if @d.count > 0 # Vertica will throw an error if the table has just been created and does not have a super projection yet.
  end

  specify "should quote columns and tables using double quotes if quoting identifiers" do
    @d.select(:name).sql.should == \
      'SELECT "name" FROM "test"'

    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM "test"'

    @d.select(:max.sql_function(:value)).sql.should == \
      'SELECT max("value") FROM "test"'

    @d.select(:NOW.sql_function).sql.should == \
    'SELECT NOW() FROM "test"'

    @d.select(:max.sql_function(:items__value)).sql.should == \
      'SELECT max("items"."value") FROM "test"'

    @d.order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.select('test.name AS item_name'.lit).sql.should == \
      'SELECT test.name AS item_name FROM "test"'

    @d.select('"name"'.lit).sql.should == \
      'SELECT "name" FROM "test"'

    @d.select('max(test."name") AS "max_name"'.lit).sql.should == \
      'SELECT max(test."name") AS "max_name" FROM "test"'

    @d.insert_sql(:x => :y).should =~ \
      /\AINSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?\z/

  end

  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.reverse_order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" ASC'

    @d.reverse_order(:name, :test.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'

    @d.reverse_order(:name.desc, :test).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
  end

  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}

    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end

  specify "#columns should return the correct column names" do
    @d.columns!.should == [:name, :value]
    @d.select(:name).columns!.should == [:name]
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

  cspecify "should store milliseconds in time fields for Time objects", :do, :swift do
    t = Time.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>1][:time]
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    (t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).should == t.usec
  end

  cspecify "should store milliseconds in time fields for DateTime objects", :do, :swift do
    t = DateTime.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>1][:time]
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    (t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).should == t.strftime('%N').to_i/1000
  end

  describe "Verticas's EXPLAIN and EXPLAIN LOCAL" do
    specify "should not raise errors" do
      @d = VERTICA_DB[:test3]
      proc{@d.explain}.should_not raise_error
      proc{@d.explain(:local => true)}.should_not raise_error
    end
  end

end


describe "A Vertica database" do
  before do
    @db = VERTICA_DB
  end

  specify "should support column operations" do
    @db.create_table!(:test2){varchar :name; integer :value}
    @db[:test2] << {}
    @db[:test2].columns.should == [:name, :value]

    @db.add_column :test2, :xyz, :varchar, :default => '000'
    @db[:test2].columns.should == [:name, :value, :xyz]
    @db[:test2] << {:name => 'mmm', :value => 111}
    @db[:test2].first[:xyz].should == '000'

    @db[:test2].columns.should == [:name, :value, :xyz]
    proc{ @db.drop_column :test2, :xyz }.should raise_error(Sequel::DatabaseError,
                                                    /ALTER TABLE DROP COLUMN not supported/)

    @db[:test2].columns.should ==[:name, :value, :xyz]

    @db[:test2].delete
    @db.add_column :test2, :xyz2, :varchar, :default => '000'
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz2 => 'qqqq'}

    @db[:test2].columns.should == [:name, :value, :xyz, :xyz2]
    @db.rename_column :test2, :xyz, :zyx
    @db[:test2].columns.should == [:name, :value, :zyx, :xyz2]
    @db[:test2].first[:xyz2].should == 'qqqq'

    @db.add_column :test2, :xyz, :float
    @db[:test2].delete
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 56.78}

    proc{ @db.set_column_type :test2, :xyz, :integer }.should raise_error(Sequel::DatabaseError,
                                                    /ALTER TABLE ALTER COLUMN not supported/)
  end

  specify "#locks should be a dataset returning database locks " do
    @db.locks.should be_a_kind_of(Sequel::Dataset)
    @db.locks.all.should be_a_kind_of(Array)
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

  specify "should work with static SQL" do
    @ds.with_sql('INSERT INTO test5 (value) VALUES (10)').insert.should == 1
    @db['INSERT INTO test5 (value) VALUES (20)'].insert.should == 1
    @ds.all.should == [{:value=>10}, {:value=>20}]
  end

  specify "should insert correctly if using a column array and a value array" do
    @ds.insert([:value], [10]).should == 1
    @ds.all.should == [{:value=>10}]
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
    VERTICA_DB.default_schema = nil
  end

  specify "should be able to create, drop, select and insert into tables in a given schema" do
    VERTICA_DB.create_table(:schema_test__table_in_schema_test){integer :i}
    VERTICA_DB[:schema_test__table_in_schema_test].first.should == nil
    VERTICA_DB[:schema_test__table_in_schema_test].insert(:i=>1).should == 1
    VERTICA_DB[:schema_test__table_in_schema_test].first.should == {:i=>1}
    VERTICA_DB.from('schema_test.table_in_schema_test'.lit).first.should == {:i=>1}
    VERTICA_DB.drop_table(:schema_test__table_in_schema_test)
    VERTICA_DB.create_table(:table_in_schema_test.qualify(:schema_test)){integer :i}
    VERTICA_DB[:schema_test__table_in_schema_test].first.should == nil
    VERTICA_DB.from('schema_test.table_in_schema_test'.lit).first.should == nil
    VERTICA_DB.drop_table(:table_in_schema_test.qualify(:schema_test))
  end

  specify "#tables should not include tables in a default non-public schema" do
    VERTICA_DB.create_table(:schema_test__table_in_schema_test){integer :i}
    VERTICA_DB.tables.should include(:table_in_schema_test)
    VERTICA_DB.tables.should_not include(:tables)
    VERTICA_DB.tables.should_not include(:columns)
    VERTICA_DB.tables.should_not include(:locks)
    VERTICA_DB.tables.should_not include(:domain_udt_usage)
  end

  specify "#tables should return tables in the schema provided by the :schema argument" do
    VERTICA_DB.create_table(:schema_test__table_in_schema_test){integer :i}
    VERTICA_DB.tables(:schema=>:schema_test).should == [:table_in_schema_test]
  end

  specify "#schema should not include columns from tables in a default non-public schema" do
    VERTICA_DB.create_table(:schema_test__domains){integer :i}
    sch = VERTICA_DB.schema(:domains)
    cs = sch.map{|x| x.first}
    cs.should include(:i)
    cs.should_not include(:data_type)
  end

  specify "#schema should only include columns from the table in the given :schema argument" do
    VERTICA_DB.create_table!(:domains){integer :d}
    VERTICA_DB.create_table(:schema_test__domains){integer :i}
    sch = VERTICA_DB.schema(:domains, :schema=>:schema_test)
    cs = sch.map{|x| x.first}
    cs.should include(:i)
    cs.should_not include(:d)
    VERTICA_DB.drop_table(:domains)
  end

  specify "#table_exists? should see if the table is in a given schema" do
    VERTICA_DB.create_table(:schema_test__schema_test){integer :i}
    VERTICA_DB.table_exists?(:schema_test__schema_test).should == true
  end

end
