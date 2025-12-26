{
  description = "Nix flake for the todo CLI";

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
        venv = pythonSet.mkVirtualEnv "todo-venv" workspace.deps.default;
        todoApp = pkgs.writeShellScriptBin "todo" ''
          cd ${workspaceRoot}
          export PYTHONPATH=${workspaceRoot}
          exec ${venv}/bin/python main.py "$@"
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.uv
            venv
          ];
          shellHook = ''
            export PYTHONPATH=${workspaceRoot}
          '';
        };
        packages.todo = todoApp;
        apps.default = {
          type = "app";
          program = "${todoApp}/bin/todo";
        };
      });
}
