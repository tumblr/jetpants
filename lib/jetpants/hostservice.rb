require 'hostservice/upstart'
require 'hostservice/systemd'

module Jetpants
  module HostService
    def self.pick_by_preflight(host)
      # We want to pick the first provider which the machine supports. Ruby 1.9.2 has no
      # `first_where` method like
      #    [1, 2, 3].first { |i| i > 1 } == 2
      # but the next line fakes it, by deleting items until we find one which does match, then
      # taking the first item in the array.
      # This could be a `.map` or a `.select` but we really don't want to try any more than we
      # have to.
      provider = all_providers.drop_while { |candidate| ! candidate.preflight(host) }.first
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
