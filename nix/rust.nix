{ sources ? import ./sources.nix }:

let
  pkgs = import sources.nixpkgs {
    overlays = [ (import sources.rust-overlay) ];
  };
  overrideSet = {
    extensions = [ "rust-analyzer" "rust-src" ] ++ (if useMiri then [ "miri" ] else [ ]);
    # targets = [ "riscv64gc-unknown-none-elf" "x86_64-unknown-linux-gnu" ];
  };
  nightlyToolchain = toolchain: toolchain.default.override overrideSet;
  nightlyRust = with pkgs; rust-bin.selectLatestNightlyWith nightlyToolchain;
  stableRust = pkgs.rust-bin.stable.latest.default.override overrideSet;

  useMiri = true;
in
if useMiri then nightlyRust else stableRust
