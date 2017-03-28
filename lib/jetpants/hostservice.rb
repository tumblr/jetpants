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

      # We want to pick the first provider which the machine supports. Ruby 1.9.2 has no
      # `first_where` method like
      #    [1, 2, 3].first { |i| i > 1 } == 2
      # but the next line fakes it, by deleting items until we find one which does match, then
      # taking the first item in the array.
      # This could be a `.map` or a `.select` but we really don't want to try any more than we
      # have to.
      provider = all_providers.drop_while { |candidate| ! candidate.preflight(host, pid1_name) }.first
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
