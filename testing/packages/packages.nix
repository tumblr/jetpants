{
  # packageOverrides allows us to take the set of packages and change
  # them, override their definitions, or even add our own.

  # The specific syntax here and the callPackage isn't specifically
  # important, just know that when it is calling `./docker` it by
  # default loads `./docker/default.nix`.
  nixpkgs.config.packageOverrides = super: let self = super.pkgs; in {
    # The current version of nixpkgs contains a broken set of
    # dockerTools, which I was able to fix for this demo. I won't
    # document that code in this example.

    # The dockerTools are being replaced with something better
    # upstream.
    dockerTools = self.callPackage ./docker { };

    # mutatedScript is a tool which takes in a shell script off the
    # filesystem and allows you to edit it ways, like replacing
    # segments. By default, it also runs `shellcheck` on the script
    # and won't allow you to build unless it passes muster.
    mutatedScript = self.callPackage ./mutated-script { };

    # [jetpants installs jetpants and all of its
    # dependencies.](./jetpants/default.html)
    jetpants = self.callPackage ./jetpants { };

    # [collins-client installs the collins CLI client and all of its
    # dependencies.](./collins-client/default.html)
    collins-client = self.callPackage ./collins-client { };

    # Okay, this one is a bit weird / complicated, but [the collins
    # package](./collins-container/default.html) is actually going to
    # resolve to a `.tar.gz` on disk of the docker container as loaded
    # from Docker Hub. This will then get loaded by docker in the
    # [collins-container service](./collins-container/service.html).
    collins-container = self.callPackage ./collins-container { };

    netcat = self.netcat-openbsd;

    percona-server56 = self.callPackage ./percona-server/5.6.x.nix { };

    percona-toolkit = self.callPackage ./percona-toolkit { };

    service-stub = self.callPackage ./service-stub { };
  };
}
