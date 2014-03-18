# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    @enabled_secondary_retries_once = false

    # Override Thor.dispatch to use separate retry settings for potentially long-running
    # console and Thor command tasks, as well as allowing simple callbacks identically
    # to those defined in bin/jetpants
    def self.dispatch(task, given_args, given_ops, config)
      if !@enabled_secondary_retries_once
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

      task_name = task || given_args[0]
      self.send "before_#{task_name}" if self.respond_to? "before_#{task_name}"
      super
      self.send "after_#{task_name}" if self.respond_to? "after_#{task_name}"
    end

    desc 'create_pool', 'create a new database pool'
    method_option :name, :name => 'unique name of new pool to be created'
    method_option :master, :master => 'ip of pre-configured master for new pool'
    def create_pool
      name = options[:name] || ask('Please ender the name of the new pool.')
      if configuration_assets('MYSQL_POOL').map(&:pool).include? name.upcase
        error "Pool #{name} already exists"
      end
      master = options[:master] ||
               ask("Please enter the ip of the master, or 'none' if one does not yet exist.")
      if (master.downcase != 'none') && ! is_ip? master
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
 
  end
end
