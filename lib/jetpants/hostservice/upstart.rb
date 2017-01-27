module Jetpants
  module HostService
    class Upstart
      def self.preflight(host)
        host.has_installed('service')
      end

      def initialize(host)
        @host = host
      end

      def start(name, options=[])
        service(:start, name, options.join(' '))
      end

      def restart(name, options=[])
        service(:restart, name, options.join(' '))
      end

      def stop(name)
        service(:stop, name)
      end

      def running?(name)
        status = service(:status, name).downcase
        # the service is running if the output of "service #{name} status" doesn't include any of
        # these strings
        not_running_strings = ['not running', 'stop/waiting']

        not_running_strings.none? {|str| status.include? str}
      end

      def service_direct(operation, name, options='')
        service(operation, name, options)
      end

      # Performs the given operation (:start, :stop, :restart, :status) for the
      # specified service (ie "mysql"). Requires that the "service" bin is in
      # root's PATH.
      # Please be aware that the output format and exit codes for the service
      # binary vary between Linux distros!
      def service(operation, name, options='')
        @host.ssh_cmd "service #{name} #{operation.to_s} #{options}".rstrip
      end
    end
  end
end
