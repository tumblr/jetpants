require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'alter_table', 'perform an alter table'
    method_option :pool, :desc => 'Name of pool to run the alter table on'
    method_option :dry_run, :desc => 'Dry run of the alter table', :type => :boolean
    method_option :alter, :desc => 'The alter statement (eg ADD COLUMN c1 INT)'
    method_option :database, :desc => 'Database to run the alter table on'
    method_option :table, :desc => 'Table to run the alter table on'
    method_option :all_shards, :desc => 'To run on all the shards', :type => :boolean
    method_option :no_check_plan, :desc => 'Do not check the query execution plan', :type => :boolean
    method_option :shard_pool, :desc => 'The sharding pool for which to perform the alter'
    def alter_table
      unless options[:all_shards]
        pool_name = options[:pool] || ask('Please enter a name of a pool: ')
        pool = Jetpants.topology.pool(pool_name)
        raise "#{pool_name} is not a pool name" unless pool
      end

      database = options[:database] || false
      table = options[:table] || ask('Please enter a name of a table: ')
      alter = options[:alter] || ask('Please enter a alter table statement (eg ADD COLUMN c1 INT): ')

      if options[:all_shards]
        shard_pool = options[:shard_pool] || ask('Please enter the sharding pool for which to perform the split (enter for default pool): ')
        shard_pool = default_shard_pool if shard_pool.empty?
        Jetpants.topology.alter_table_shards(database, table, alter, options[:dry_run], options[:no_check_plan], shard_pool)
      else
        unless pool.alter_table(database, table, alter, options[:dry_run], false, options[:no_check_plan])
          output "Check for errors during online schema change".red, :error
        end
      end
    end

    desc 'alter_table_drop', 'drop the old table after the alter table is complete'
    method_option :pool, :desc => 'Name of pool that your ran the alter table on'
    method_option :table, :desc => 'Table you ran the alter table on'
    method_option :database, :desc => 'Database you ran the alter table on'
    method_option :all_shards, :desc => 'To run on all the shards', :type => :boolean
    method_option :shard_pool, :desc => 'The sharding pool for which to drop the old table' 
    def alter_table_drop
      unless options[:all_shards]
        pool_name = options[:pool] || ask('Please enter a name of a pool: ')
        pool = Jetpants.topology.pool(pool_name)
        raise "#{pool_name} is not a pool name" unless pool
      end

      database = options[:database] || false
      table = options[:table] || ask('Please enter a name of a table: ')

      if options[:all_shards]
        shard_pool = options[:shard_pool] || ask('Please enter the sharding pool for which to perform the split (enter for default pool): ')
        shard_pool = default_shard_pool if shard_pool.empty?
        Jetpants.topology.drop_old_alter_table_shards(database, table, shard_pool)
      else
        pool.drop_old_alter_table(database, table)
      end
    end

  end
end
