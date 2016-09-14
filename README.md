# Sequel-vertica [![Build Status](https://travis-ci.org/camilo/sequel-vertica.svg?branch=master)](https://travis-ci.org/camilo/sequel-vertica)

A third party adapter to use Vertica through sequel, most of the actual work is
done by sequel and the vertica gem.

## Usage

The usage is straight forward as any other Sequel adapter, just make sure to
require sequel and the sequel-vertica gem.

```ruby 
require 'sequel'
require 'sequel-vertica'

$DB = Sequel.connect('vertica://user:pw@host/database_name')
```
