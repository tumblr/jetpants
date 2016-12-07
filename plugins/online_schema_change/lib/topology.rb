module Jetpants
  class Topology

    # run an alter table on all the sharded pools
    # if you specify dry run it will run a dry run on all the shards
    # otherwise it will run on the first shard and ask if you want to
    # continue on the rest of the shards, 10 shards at a time
    def alter_table_shards(database, table, alter, dry_run=true, shard_pool=nil, skip_rename=false, arbitrary_options=[])
      shard_pool = Jetpants.topology.default_shard_pool if shard_pool.nil?
      my_shards = shards(shard_pool).dup


      ui = PreflightShardUI.new(my_shards)
      ui.run! do |shard,stage|
        # If we're past preflight, we want to not prompt the confirmation.
        force = stage == :all
        shard.alter_table(database, table, alter, dry_run, force, skip_rename, arbitrary_options)
      end
    end

    # will drop old table from the shards after a alter table
    # this is because we do not drop the old table in the osc
    # also I will do the first shard and ask if you want to
    # continue, after that it will do each table serially
    def drop_old_alter_table_shards(database, table, shard_pool = nil)
      shard_pool = Jetpants.topology.default_shard_pool if shard_pool.nil?
      my_shards = shards(shard_pool).dup

      ui = PreflightShardUI.new(my_shards)
      ui.run! { |shard,_| shard.drop_old_alter_table(database, table) }
    end

    def rename_table_shards(database, orig_table, copy_table, shard_pool=nil)
      shard_pool = Jetpants.topology.default_shard_pool if shard_pool.nil?
      my_shards = shards(shard_pool).dup

      ui = PreflightShardUI.new(my_shards)
      ui.run! { |shard,_| shard.rename_table(database, orig_table, copy_table) }
    end
  end
end
