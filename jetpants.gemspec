require 'rake'

Gem::Specification.new do |s|
  s.name = "jetpants"
  s.version = "0.7.5"

  s.homepage = 'https://github.com/tumblr/jetpants/'
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.required_ruby_version = '>= 1.9.2'
  s.authors = ["Evan Elias", "Dallas Marlow"]
  s.date  = "2012-09-14"
  s.email = ["me@evanelias.com", "dallasmarlow@gmail.com"]
  s.files = FileList['Gemfile', 'README.rdoc', 'doc/*.rdoc', 'lib/**/*.rb', 'bin/**', 'plugins/**/*.rb', 'etc/jetpants.yaml.sample'].to_a
  s.require_paths = ["lib"]
  s.executables = ["jetpants"]
  s.default_executable = "jetpants"
  s.rubygems_version = "1.8.10"
  s.summary = 'Jetpants: a MySQL automation toolkit by Tumblr'
  s.description = "Jetpants is an automation toolkit for handling monstrously large MySQL database topologies. It is geared towards common operational tasks like cloning slaves, rebalancing shards, and performing master promotions. It features a command suite for easy use by operations staff, though it's also a full Ruby library for use in developing custom migration scripts and database automation."
  s.extra_rdoc_files = FileList['README.rdoc', 'doc/*.rdoc']
  s.has_rdoc = true
  s.rdoc_options = ["--line-numbers", "--title", s.summary, "--main", "README.rdoc"]

  if s.respond_to? :specification_version then
    s.specification_version = 3
    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      %w[mysql2 sequel net-ssh pry thor highline colored].each do |gem|
        s.add_runtime_dependency gem
      end
    end
  else
  end
end
