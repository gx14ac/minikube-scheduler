{
  description = "minikube-scheduler";

  inputs.nixpkgs.url = "nixpkgs/nixos-21.11";

  outputs = { self, nixpkgs }:
    let

      # Generate a user-friendly version number.
      version = builtins.substring 0 8 self.lastModifiedDate;

      # System types to support.
      supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];

      # Helper function to generate an attrset '{ x86_64-linux = f "x86_64-linux"; ... }'.
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;

      # Nixpkgs instantiated for supported system types.
      nixpkgsFor = forAllSystems (system: import nixpkgs { inherit system; });

    in
    {
        defaultPackage = forAllSystems (system: self.packages.${system}.minikube-scheduler);

        devShell = forAllSystems (system:
          let pkgs = nixpkgsFor.${system};
          in pkgs.mkShell {
            buildInputs = with pkgs; [
              go_1_17
              goimports
              gopls
            ];

            shellHook = ''
              export GOPATH=$GOPATH
              PATH=$PATH:$GOPATH/bin
              export GO111MODULE=on
            '';
          });
    };
}
