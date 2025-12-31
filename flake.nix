{
  description = "Nix flake for the tdo CLI";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
    };
  };

  outputs = inputs@{ nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
        python = pkgs.python313;
        workspaceRoot = ./.;
        workspace = inputs.uv2nix.lib.workspace.loadWorkspace {
          workspaceRoot = workspaceRoot;
        };
        overlay = workspace.mkPyprojectOverlay {
          sourcePreference = "wheel";
        };
        baseSet = pkgs.callPackage inputs.pyproject-nix.build.packages {
          inherit python;
        };
        pythonSet = baseSet.overrideScope (
          pkgs.lib.composeManyExtensions [
            inputs.pyproject-build-systems.overlays.default
            overlay
          ]
        );
        venv = pythonSet.mkVirtualEnv "tdo-venv" workspace.deps.default;
        testHash = "$2b$12$.R3ZiywXe5tgoRagYudVL.hAEGyg4H40Gv0tpSLxNN4MFobFQp3Zy";
        radicaleTest = pkgs.writeScriptBin "radicale-test" ''
#!${pkgs.bash}/bin/bash
set -euo pipefail
storage=$(mktemp -d /tmp/radicale-test-XXXXXX)
trap 'rm -rf "$storage"' EXIT
config=$(mktemp /tmp/radicale-test-config-XXXXXX)
cat <<'EOF' > "$config"
[server]
hosts = 0.0.0.0:5232

[auth]
type = htpasswd
htpasswd_filename = ${pkgs.writeText "users" "test:${testHash}\n"}
htpasswd_encryption = bcrypt

[rights]
type = authenticated

[storage]
type = multifilesystem
filesystem_folder = "$storage"
EOF
export RADICALE_CONFIG="$config"
exec ${pkgs.radicale}/bin/radicale
        '';
        tdoApp = pkgs.writeShellScriptBin "tdo" ''
          cd ${workspaceRoot}
          export PYTHONPATH=${workspaceRoot}
          exec ${venv}/bin/python main.py "$@"
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.commitizen
            pkgs.uv
            venv
          ];
          shellHook = ''
            export PYTHONPATH=${workspaceRoot}
          '';
        };
        packages.tdo = tdoApp;
        packages.radicaleTest = radicaleTest;
        apps.default = {
          type = "app";
          program = "${tdoApp}/bin/tdo";
        };
        apps.radicaleTest = {
          type = "app";
          program = "${radicaleTest}/bin/radicale-test";
        };
      });
}
