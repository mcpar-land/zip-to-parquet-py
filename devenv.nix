{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: {
  languages.nix.enable = true;
  languages.python.enable = true;
  languages.python.venv.enable = true;
  languages.python.venv.quiet = true;
  languages.python.venv.requirements = ''
    maturin
    polars
  '';
}
