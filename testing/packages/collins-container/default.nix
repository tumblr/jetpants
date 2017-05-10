{ pkgs, ident ? "oogabooga" }:
{
  # `dockerTools.pullImage` will pull the image outside of docker and
  # save it as a `.tar.gz` which we can `docker load`. The `ident`
  # could be used to refer to an image's ID, but for now it is just
  # the example string `exampleid`.

  # When referring to the collins-container package later, it will
  # look like this:
  # ```
  # pkgs.collins-container = {
  #   img = /nix/store/....tumblr-collins-exampleid.tar.gz;
  #   ident = "exampleid";
  # };
  # ```
  img = pkgs.dockerTools.pullImage {
    imageName = "tumblr/collins";
    sha256 = "0x22q0a9r885xjzc0ffl4xs9g5w66sqgxid822r874h46rpzzkzx";
    inherit ident;
  };
  inherit ident;
}
