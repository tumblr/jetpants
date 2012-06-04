require 'rake'

Gem::Specification.new do |s|
  s.name = "jetpants"
  s.version = "0.6.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.required_ruby_version = '>= 1.9.2'
  s.authors = ["Evan Elias", "Dallas Marlow"]
  s.date  = "2012-06-04"
  s.email = ["me@evanelias.com", "dallasmarlow@gmail.com"]
  s.files = FileList["Gemfile", "README.rdoc", 'doc/*', 'lib/**/*.rb', 'bin/**', 'plugins/**/*.rb', 'tasks/**', 'etc/jetpants.yaml.sample'].to_a
  s.require_paths = ["lib"]
  s.executables = ["jetpants"]
  s.default_executable = "jetpants"
  s.rubygems_version = "1.8.10"
  s.summary = 'Jetpants: a MySQL automation toolkit by Tumblr'
  s.extra_rdoc_files = FileList['README.rdoc', 'doc/*.rdoc']
  s.has_rdoc = true
  s.rdoc_options = ["--line-numbers", "--title", s.summary, "--main", "README.rdoc"]

  if s.respond_to? :specification_version then
    s.specification_version = 3
    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      %w[mysql2 sequel net-ssh state_machine pry thor highline terminal-table colored].each do |gem|
        s.add_runtime_dependency gem
      end
    end
  else
  end
end
