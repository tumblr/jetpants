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
 
  end
end
