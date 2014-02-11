# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    @enabled_secondary_retries_once = false

    # Override Thor.dispatch to allow simple callbacks, which must be before_foo / 
    # after_foo *class* methods of Jetpants::CommandSuite. 
    # These aren't as full-featured as normal Jetpants::Callback: you can only have
    # ONE before_foo or after_foo method (they override instead of stacking); no arg
    # passing; no callback abort exception type. Mostly useful for plugins overriding
    # reminder text before or after a task.
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
