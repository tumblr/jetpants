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
    sha256 = "1k9gcv9xmzawbr4dnh9pcl2akbp65h4kda9ay6k1500ag9jmjkmn";
    inherit ident;
  };
  inherit ident;
}
