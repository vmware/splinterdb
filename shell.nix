# Copyright 2026 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Enter with: nix-shell
# On this host, use /nix/var/nix/profiles/default/bin/nix-shell; the
# nix-shell on PATH is older than the host's default Nixpkgs supports.

let
  # Pin GCC, LLVM 19 and dependencies to one package collection.
  pkgs = import (builtins.fetchTarball {
    url = "https://releases.nixos.org/nixos/24.11/nixos-24.11.719113.50ab793786d9/nixexprs.tar.xz";
    sha256 = "189lwv8l10bm9fnksfkzy05pm86gmziy9z3bdn13zc8d636ylchj";
  }) {};
  clang19Compiler = pkgs.llvmPackages_19.clang.override {
    bintools = pkgs.llvmPackages_19.bintools;
  };
  clang19 = pkgs.writeShellScriptBin "clang-19" ''
    exec ${clang19Compiler}/bin/clang "$@"
  '';
  clangxx19 = pkgs.writeShellScriptBin "clang++-19" ''
    exec ${clang19Compiler}/bin/clang++ "$@"
  '';
  clangFormat19 = pkgs.writeShellScriptBin "clang-format-19" ''
    exec ${pkgs.llvmPackages_19.clang-tools}/bin/clang-format "$@"
  '';
in
(pkgs.mkShell.override { stdenv = pkgs.gcc13Stdenv; }) {
  # Nix's fortify support adds -O2, including to Splinter's debug builds.
  # Leave optimization selection to the upstream Makefile.
  hardeningDisable = [ "fortify" ];
  packages = with pkgs; [
    gnumake
    pkg-config
    clang19Compiler
    llvmPackages_19.clang-tools
    llvmPackages_19.lld
    clang19
    clangxx19
    clangFormat19
  ];
  buildInputs = with pkgs; [
    libaio
    libconfig
    xxHash
  ];
  # LLVM's propagated inputs also expose a bare clang-19. Put our versioned
  # commands first so they retain Nix's standard-header and library paths.
  shellHook = ''
    export PATH="${pkgs.lib.makeBinPath [ clang19 clangxx19 clangFormat19 clang19Compiler ]}:$PATH"
  '';
}
