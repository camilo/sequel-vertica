require 'sequel-vertica/version'
require './lib/sequel/adapters/vertica'

Sequel::Database::ADAPTERS << 'vertica'
