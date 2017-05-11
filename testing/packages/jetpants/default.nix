{ stdenv, makeWrapper, fetchFromGitHub, lib, bundlerEnv, ruby, writeScript }:

let

  version = "0.9.4";
  jetpants_src = (import ./gemset-shim.nix).jetpants.src;

  # Here we create a set of installed gems as if `bundle install` were
  # run with the specified `Gemfile` and `Gemfile.lock`. However,
  # since Nix doesn't allow any network access without first declaring
  # what the sha256 of your contents is, we provide the
  # [`gemset-shim.nix`](./gemset-shim.html). The shim file the loads
  # [`gemset.nix`](./gemset.html) which is automatically generated.
  env = bundlerEnv rec {
    name = "jetpants-${version}";

    inherit version ruby;
    gemfile = "${jetpants_src}/Gemfile";
    lockfile = ./Gemfile.lock;
    gemset = ./gemset-shim.nix;
  };

in
# stdenv.mkDerivation basically makes a package in nix, in this case
# though it is extremely simple: it creates a little wrapper program
# called `jetpants` which sets `GEM_HOME` to the `env` (from above)
# before calling the original jetpants.
stdenv.mkDerivation {
  name = "jetpants-${version}";

  buildInputs = [ makeWrapper env.ruby ];

  src = (import ./gemset-shim.nix).jetpants.src;

  dontBuild = true;

  installPhase = ''
    mkdir -p $out/bin
    cp  bin/jetpants $out/bin/jetpants
    cp Gemfile $out/Gemfile
    cp -r lib $out/lib
    cp -r plugins $out/plugins

    wrapProgram $out/bin/jetpants \
      --set GEM_HOME "${env}/${env.ruby.gemPath}"
  '';

  passthru = {
    ruby_script = name: script: writeScript "${name}" ''
      #!${env.ruby}/bin/ruby

      ENV['HOME'] = '/root'
      Gem.use_paths("${env}/${env.ruby.gemPath}", *Gem.path)

      loop do
        begin
          require 'jetpants'
          break
        rescue Exception => e
          puts "Trying after 0.5s after receiving exception: #{e.class}"
          sleep 0.5
        end
      end

      ${script}
    '';
  };

  meta = with stdenv.lib; {
    description = "A MySQL automation toolkit by Tumblr";
    homepage    = https://github.com/tumblr/jetpants/;
    platforms   = platforms.unix;
  };
}
