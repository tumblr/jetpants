module Jetpants
  module HostService
    class Systemd
      def self.preflight(host, pid1_name)
        pid1_name == "systemd"
      end

      def initialize(host)
        @host = host
      end

      def start(name, options=[])
        service(:start, name, options)
      end

      def restart(name, options=[])
        service(:restart, name, options)
      end

      def stop(name)
        service(:stop, name)
      end

      def running?(name)
        status = service(:status, name).downcase

        return !status.include?("active: inactive")
      end

      def service_direct(operation, name, options='')
        service(operation, name, [options])
      end

      def service(operation, name, options=[])
        raise "Systemd doesn't support options! :( (Passed: #{options.join(' ')})" unless options.empty?

        @host.ssh_cmd "systemctl #{operation.to_s} #{name}".rstrip
      end
    end
  end
end
