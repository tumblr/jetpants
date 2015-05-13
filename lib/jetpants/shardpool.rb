module Jetpants
  # A ShardingPool is a sharding keyspace in Jetpants that contains
  # man Shards.  All shards within the pool partition a logically coherent
  # keyspace

  class ShardPool
    include CallbackHandler
    include Output

    attr_reader :name

    def initialize(name)
      @name = name
    end

    def shards
      Jetpants.topology.shards(@name)
    end
  end
end
