# This file contains any methods we're adding to core Ruby modules

# Add Jetpants-specific conversion methods to Object.
class Object
  # Converts self to a Jetpants::DB by way of to_s. Only really useful for
  # Strings containing IP addresses, or Objects whose to_string method returns
  # an IP address as a string.
  def to_db
    if self.to_s =~ /(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/
      Jetpants::DB.new(self.to_s)
    else
      selector = {
        :hostname => "^#{self.to_s}$",
        :details => true,
        :size => 1,
        :page => 0
      }

      assets = Jetpants::Plugin::JetCollins.find(selector, false)
      raise "Invalid hostname: #{self}" if assets.empty?

      assets.first.to_db
    end
  end
end
