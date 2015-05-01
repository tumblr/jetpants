# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    @enabled_secondary_retries_once = false

    # Hook into before_dispatch to use separate retry settings for
    # potentially long-running console and Thor command tasks
    def self.jetpants_collins_before_dispatch(task)
      unless @enabled_secondary_retries_once
        Jetpants.plugins['jetpants_collins']['retries'] =
          Jetpants.plugins['jetpants_collins']['retries_interactive'] ||
          Jetpants.plugins['jetpants_collins']['retries']             ||
          23 # 303 seconds if max_retry_backoff is 16

        Jetpants.plugins['jetpants_collins']['max_retry_backoff'] =
          Jetpants.plugins['jetpants_collins']['max_retry_backoff_interactive'] ||
          Jetpants.plugins['jetpants_collins']['max_retry_backoff']             ||
          16

        @enabled_secondary_retries_once = true
      end
    end

    desc 'create_pool', 'create a new database pool'
    method_option :name, :name => 'unique name of new pool to be created'
    method_option :master, :master => 'ip of pre-configured master for new pool'
    def create_pool
      name = options[:name] || ask('Please enter the name of the new pool: ')
      if Jetpants.topology.configuration_assets('MYSQL_POOL').map(&:pool).include? name.upcase
        error "Pool #{name} already exists"
      end
      master = options[:master] || ask("Please enter the ip of the master, or 'none' if one does not yet exist: ")
      if (master.downcase != 'none') && ! (is_ip? master)
        error "Master must either be 'none' or a valid ip."
      end

      master = master == 'none' ? nil : master
      new_pool = Pool.new(name, master || nil)
      new_pool.sync_configuration

      application = case agree("Would you like to set a collins application attribute? [yes/no]")
      when "yes"
        ask('What is the name of the collins application for this pool?')
      end

      if application and application.length == 0
        error 'Application must not be empty.'
      else
        new_pool.collins_set(:application => application)
      end
    end

    desc 'pause_pool_replication', 'pause replication on all slaves in a given pool'
    method_option :pool
    def pause_pool_replication
      pool = Jetpants.topology.pool (options[:pool] || ask('Please enter the pool name: ')).downcase
      error "Unable to find pool" unless pool

      master = pool.master
      error "Unable to find master for pool" unless master

      slaves = master.slaves
      error "Master does not have any replicating slaves to pause" unless slaves and not slaves.empty?

      inform "Pausing replication on #{slaves.size} slaves"
      slaves.concurrent_each do |slave|
        slave.disable_monitoring rescue inform "Warning! Unable to disable monitoring for slave: #{slave}"
        slave.pause_replication  rescue inform "Warning! Unable to pause replication for slave: #{slave}"
      end
    end

    desc 'resume_pool_replication', 'resume replication on all slaves according to collins for a given pool'
    method_option :pool
    def resume_pool_replication
      pool = Jetpants.topology.pool (options[:pool] || ask('Please enter the pool name: ')).downcase
      error "Unable to find pool" unless pool

      master = pool.master
      error "Unable to find master for pool" unless master

      slaves = pool.slaves_according_to_collins
      error "Unable to find any slaves (via collins) for pool" unless slaves and not slaves.empty?

      inform "Resuming replication on #{slaves.size} slaves"
      slaves.concurrent_each(&:resume_replication)

      inform "Preparing to enable monitoring after slaves catch up to master"
      slaves.concurrent_each do |slave|
        slave.catch_up_to_master
        slave.enable_monitoring
      end
    end
  end
end
