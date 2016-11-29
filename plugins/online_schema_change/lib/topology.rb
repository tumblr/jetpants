module Jetpants
  class Topology

    # run an alter table on all the sharded pools
    # if you specify dry run it will run a dry run on all the shards
    # otherwise it will run on the first shard and ask if you want to
    # continue on the rest of the shards, 10 shards at a time
    def alter_table_shards(database, table, alter, dry_run=true, shard_pool=nil, skip_rename=false, arbitrary_options=[])
      shard_pool = Jetpants.topology.default_shard_pool if shard_pool.nil?
      my_shards = shards(shard_pool).dup
      first_shard = my_shards.shift
      output "Will run on first shard and prompt for going past the dry run only on the first shard\n\n"
      output "#{first_shard.pool.to_s}\n"
      unless first_shard.alter_table(database, table, alter, dry_run, false, skip_rename, arbitrary_options)
        output "First shard had an error, please check output\n"
        return
      end

      continue = ask('First shard complete would you like to continue with the rest of the shards?: (YES/no) - YES has to be in all caps and fully typed')
      if continue == 'YES'
        errors = []
        exceptions = {}

        my_shards.limited_concurrent_map(10) do |shard|
          output "#{shard.pool.to_s}\n"
          begin
            errors << shard unless shard.alter_table(database, table, alter, dry_run, true, skip_rename, arbitrary_options)
          rescue Exception => e
            output "#{shard.pool.to_s} threw exception: #{e}".red
            errors << shard
            exceptions[shard.name] << e
          end
        end

        errors.each do |shard|
          output "check #{shard.name} for errors during online schema change\n"
        end

        exceptions.each do |pool, exception|
          output "Pool '#{pool}' threw exception: #{exception}".red
          output "Backtrace:"
          output e.backtrace.join("\n\t").red
        end
      end
    end

    # will drop old table from the shards after a alter table
    # this is because we do not drop the old table in the osc
    # also I will do the first shard and ask if you want to
    # continue, after that it will do each table serially
    def drop_old_alter_table_shards(database, table, shard_pool = nil)
      shard_pool = Jetpants.topology.default_shard_pool if shard_pool.nil?
      my_shards = shards(shard_pool).dup
      first_shard = my_shards.shift
      output "Will run on first shard and prompt before going on to the rest\n\n"
      output "#{first_shard.pool.to_s}\n"
      first_shard.drop_old_alter_table(database, table)

      continue = ask('First shard complete would you like to continue with the rest of the shards?: (YES/no) - YES has to be in all caps and fully typed')
      if continue == 'YES'
        my_shards.each do |shard|
          print "#{shard.pool.to_s}\n"
          shard.drop_old_alter_table(database, table)
        end
      end
    end

    def rename_table_shards(database, orig_table, copy_table, shard_pool=nil)
      shard_pool = Jetpants.topology.default_shard_pool if shard_pool.nil?
      my_shards = shards(shard_pool).dup
      first_shard = my_shards.shift
      output "Will run on first shard and prompt before going on to the rest\n\n"
      output "#{first_shard.pool.to_s}\n"
      first_shard.rename_table(database, orig_table, copy_table)

      continue = ask('First shard complete would you like to continue with the rest of the shards?: (YES/no) - YES has to be in all caps and fully typed')
      if continue == 'YES'
        my_shards.each do |shard|
          print "#{shard.pool.to_s}\n"
          shard.rename_table(database, orig_table, copy_table)
        end
      end
    end

  end
end
