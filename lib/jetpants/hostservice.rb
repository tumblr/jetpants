require 'hostservice/upstart'
require 'hostservice/systemd'

module Jetpants
  module HostService
    def self.pick_by_preflight(host)
      # In /proc/1/stat we get to see the name of the init process (ie: pid1) so
      # we can best match it to our providers.
      #
      # We expect to see something like:
      # 1 (process_name) S 0 1 1 0 -1 4202752 2925 7358649115 10 1047 604 1079 [...snipped...]
      #
      # on upstart, we'd see:
      # 1 (init) S 0 1 1 0 -1 4202752 2925 7358649115 10 1047 604 1079 [...snipped...]
      #
      # on systemd we'd see:
      # 1 (systemd) S 0 1 1 0 -1 4219136 2502297 91460739 50 3010 3771 3113 [...snipped...]
      #
      # For more information, see proc(5) (`man proc`) and look for `/proc/[pid]/stat`.
      pid1_match = host.ssh_cmd('cat /proc/1/stat').match(/^1 \((.*?)\) /)
      throw "/proc/1/stat isn't matching, something is wrong!" if pid1_match.nil?
      pid1_name = pid1_match[1]

      provider = all_providers.find { |candidate| candidate.preflight(host, pid1_name) }
      raise "Cannot detect a valid service provider for #{host}" if provider.nil?

      return provider.new(host)
    end

    def self.all_providers
      # Service managers that we can support, in order of most to least likely
      [
        Jetpants::HostService::Upstart,
        Jetpants::HostService::Systemd,
      ]
    end
  end
end
