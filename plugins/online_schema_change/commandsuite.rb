require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'alter_table', 'perform an alter table'
    method_option :pool, :desc => 'Name of pool to run the alter table on'
    method_option :dry_run, :desc => 'Dry run of the alter table', :type => :boolean
    method_option :alter, :desc => 'The alter statment (eg ADD COLUMN c1 INT)'
    method_option :database, :desc => 'Database to run the alter table on'
    method_option :table, :desc => 'Table to run the alter table on'
    method_option :all_shards, :desc => 'To run on all the shards', :type => :boolean
    def alter_table
      unless options[:all_shards]
        pool_name = options[:pool] || ask('Please enter a name of a pool: ')
        pool = Jetpants.topology.pool(pool_name)
        raise "#{pool_name} is not a pool name" unless pool
      end

      database = options[:database] || false
      table = options[:table] || ask('Please enter a name of a table: ')
      alter = options[:alter] || ask('Please enter a alter table statment (eg ADD COLUMN c1 INT): ')

      if options[:all_shards]
        Jetpants.topology.alter_table_shards(database, table, alter, options[:dry_run])
      else
        unless pool.alter_table(database, table, alter, options[:dry_run])
          print "check for errors during online schema change\n"
        end
      end
    end

    desc 'alter_table_drop', 'drop the old table after the alter table is complete'
    method_option :pool, :desc => 'Name of pool that your ran the alter table on'
    method_option :table, :desc => 'Table you ran the alter table on'
    method_option :database, :desc => 'Database you ran the alter table on'
    method_option :all_shards, :desc => 'To run on all the shards', :type => :boolean
    def alter_table_drop
      unless options[:all_shards]
        pool_name = options[:pool] || ask('Please enter a name of a pool: ')
        pool = Jetpants.topology.pool(pool_name)
        raise "#{pool_name} is not a pool name" unless pool
      end

      database = options[:database] || false
      table = options[:table] || ask('Please enter a name of a table: ')

      if options[:all_shards]
        Jetpants.topology.drop_old_alter_table_shards(database, table)
      else
        pool.drop_old_alter_table(database, table)
      end
    end

  end
end