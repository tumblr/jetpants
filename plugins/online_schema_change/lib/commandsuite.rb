require 'thor'
require 'pp'

module Jetpants
  class CommandSuite < Thor

    desc 'alter_table', 'perform an alter table, can use like  "alter_table -- --arbitrary --arguments -to --to pt-online-schema-change"'
    method_option :pool, :desc => 'Name of pool to run the alter table on'
    method_option :dry_run, :desc => 'Dry run of the alter table', :type => :boolean
    method_option :alter, :desc => 'The alter statement (eg ADD COLUMN c1 INT)'
    method_option :database, :desc => 'Database to run the alter table on'
    method_option :table, :desc => 'Table to run the alter table on'
    method_option :all_shards, :desc => 'To run on all the shards', :type => :boolean
    method_option :skip_rename, :desc => 'Perform the alter but do not replace the production table (also leaves triggers in place!)', :type => :boolean
    method_option :shard_pool, :desc => 'The sharding pool for which to perform the alter'
    def alter_table(*arbitrary_options)
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
        Jetpants.topology.alter_table_shards(database, table, alter, options[:dry_run], shard_pool, options[:skip_rename], arbitrary_options)
      else
        unless pool.alter_table(database, table, alter, options[:dry_run], false, options[:skip_rename], arbitrary_options)
          output "Check for errors during online schema change".red, :error
        end
      end
    end

    desc 'alter_table_drop', 'drop the old table after the alter table is complete'
    method_option :pool, :desc => 'Name of pool that you ran the alter table on'
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
        shard_pool = options[:shard_pool] || ask('Please enter the sharding pool for which to perform the table drop (enter for default pool): ')
        shard_pool = default_shard_pool if shard_pool.empty?
        Jetpants.topology.drop_old_alter_table_shards(database, table, shard_pool)
      else
        pool.drop_old_alter_table(database, table)
      end
    end

    desc 'alter_table_rename', 'perform table swap using RENAME TABLE command'
    method_option :pool, :desc => 'Name of pool that you ran the alter table on'
    method_option :orig_table, :desc => 'Table you ran the alter table on'
    method_option :copy_table, :desc => 'Copy table created by pt-online-schema-change'
    method_option :database, :desc => 'Database you ran the alter table on'
    method_option :all_shards, :desc => 'To run on all the shards', :type => :boolean
    method_option :shard_pool, :desc => 'The sharding pool for which to swap the tables'
    def alter_table_rename
      unless options[:all_shards]
        pool_name = options[:pool] || ask('Please enter a name of a pool: ')
        pool = Jetpants.topology.pool(pool_name)
        raise "#{pool_name} is not a pool name" unless pool
      end

      database = options[:database] || false
      orig_table = options[:orig_table] || ask('Please enter a name of a Original table: ')
      copy_table = options[:copy_table] || ask('Please enter the name of Copy table')

      if options[:all_shards]
        shard_pool = options[:shard_pool] || ask('Please enter the sharding pool for which to perform the table swap (enter for default pool): ')
        shard_pool = default_shard_pool if shard_pool.empty?
        Jetpants.topology.rename_table_shards(database, orig_table, copy_table, shard_pool)
      else
        pool.rename_table(database, orig_table, copy_table)
      end
    end

  end
end
