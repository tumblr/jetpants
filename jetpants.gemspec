require 'date'

Gem::Specification.new do |s|
  s.name = "jetpants"
  s.version = "0.9.4"

  s.homepage = 'https://github.com/tumblr/jetpants/'
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.required_ruby_version = '>= 1.9.2'
  s.authors = ["Evan Elias", "Dallas Marlow", "Bob Patterson Jr.", "Tom Christ", "Kiril Angov", "Amar Mudrankit", "Tyler Neely", "Duan Wang"]
  s.date  = Date.today.to_s
  s.email = ["me@evanelias.com", "dallasmarlow@gmail.com", "bob@bobpattersonjr.com", "tbchrist@gmail.com", "t@jujit.su", "kiril.angov@gmail.com", "amar.mudrankit@gmail.com"]
  s.files = Dir['Gemfile', 'README.rdoc', 'doc/*.rdoc', 'lib/**/*.rb', 'bin/**', 'scripts/*.rb', 'plugins/**/*.rb', 'etc/jetpants.yaml.sample'].to_a
  s.require_paths = ["lib"]
  s.executables = ["jetpants"]
  s.default_executable = "jetpants"
  s.summary = 'Jetpants: a MySQL automation toolkit by Tumblr'
  s.description = "Jetpants is an automation toolkit for handling monstrously large MySQL database topologies. It is geared towards common operational tasks like cloning slaves, rebalancing shards, and performing master promotions. It features a command suite for easy use by operations staff, though it's also a full Ruby library for use in developing custom migration scripts and database automation."
  s.extra_rdoc_files = Dir['README.rdoc', 'doc/*.rdoc'].to_a
  s.has_rdoc = true
  s.rdoc_options = ["--line-numbers", "--title", s.summary, "--main", "README.rdoc"]

  s.add_runtime_dependency 'mysql2', '~> 0.3.0'
  s.add_runtime_dependency 'sequel', '~> 3.36'
  s.add_runtime_dependency 'net-ssh', '~> 2.3'
  s.add_runtime_dependency 'pry', '~> 0.9.8'
  s.add_runtime_dependency 'pry-rescue', '~> 1.4.0'
  s.add_runtime_dependency 'thor', '~> 0.15'
  s.add_runtime_dependency 'highline', '~> 1.6.12'
  s.add_runtime_dependency 'colored', '~> 1.2'
  s.add_runtime_dependency 'collins_client', '~> 0.2.15'
  s.add_runtime_dependency 'bloom-filter', '~> 0.2.0'
  s.add_runtime_dependency 'pony', '~> 1.11'
end
