require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'capacity_snapshot', 'create a snapshot of the current useage'
    def capacity_snapshot
      Plugin::Capacity.new().snapshot
    end

    desc 'capacity_plan', 'capacity plan'
    method_option :email, :email => 'email address to send capacity plan report to'
    def capacity_plan
      email = options[:email] || false
      Plugin::Capacity.new().plan(email)
    end

  end
end