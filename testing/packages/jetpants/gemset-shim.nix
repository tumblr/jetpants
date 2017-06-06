let
  inherit (builtins) filterSource pathExists trace;

  traceInPlace = v: trace v v;
  generated = import ./gemset.nix;

  overrides = {
    jetpants = {
      source = {};
      version = "0.9.4";
      src = filterSource
        (path: type:
          # filterSource will pass us:
          # - path (ex: /home/graham/jetpants/plugins)
          # - type (ex: regular, directory)
          #
          # where we can filter out undesirable files. If we return true, the file will be included
          # in the source. If we return false, we reject it and it won't be part of the source,
          # or included in the calculation of the hash.
          #
          # If we return false on a directory, filterSource will not ask us about contents of that
          # directory.
          #
          # Not we negate the outer statement so our internal statements return true if we do not
          # want it. I think this makes it more understandable.
          !(
            # Remove any .git directory:
            (type == "directory" && baseNameOf path == ".git")

            # Ignore any directory with a .nixignore file:
            || (type == "directory" && pathExists "${path}/.nixignore")

            # Ignore the top level shell.nix
            || (type == "regular" && baseNameOf path == "shell.nix")

            # Can be helpful to enable this trace line if making fancier ignore rules.
            # || (builtins.trace path (builtins.trace type false))

            || false # fallthrough: false means don't reject
          )
        )
        # This resolves to relative path where the jetpants repo starts
        ./../../..;
    };
  };
in generated // overrides
# { foo = "foo"; } // { bar = "bar"; } == { foo = "foo"; bar = "bar" }
# so this is just overriding the jetpants definition from
# [`./gemset.nix`](./gemset.html).
