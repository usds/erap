{ pkgs ? import <nixpkgs> {} }:

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {};
  gisEnv = pkgs.poetry2nix.mkPoetryEnv {
    projectDir = ./arp_pipeline/.;
    editablePackageSources = {
      explort_gis = ./arp_pipeline/.;
    };
    overrides = [
      pkgs.poetry2nix.defaultPoetryOverrides
      (self: super: {
        faker = super.faker.override { preferWheel = true; };
      })
    ];


  };
in pkgs.mkShell {
  buildInputs = [
    pkgs.python3
    pkgs.poetry
    pkgs.python39Packages.pandas
    pkgs.python39Packages.geopandas
    pkgs.python39Packages.openpyxl

    sources.poetry2nix
    pkgs.stdenv.cc.cc.lib
    pkgs.glibc

    pkgs.mypy
    pkgs.pgcli
    pkgs.gdal_2
    pkgs.pg_top
    pkgs.tokei
    #gisEnv
    (pkgs.postgresql_13.withPackages (p: [ p.postgis ]) )

    # keep this line if you use bash
    pkgs.bashInteractive
  ];
  system = builtins.currentSystem;
}
