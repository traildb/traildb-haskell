TrailDB Haskell bindings
========================

![TrailDB logo](traildb_logo_512.png?raw=true)

These are the official Haskell bindings to [TrailDB](http://traildb.io/). Most
of the API is covered. Check out System.TrailDB module for some examples and
documentation.

How to build
------------

These bindings can be installed using `cabal-install` or `stack` on Linux.

Fetch the code in some way. You can clone
git://github.com/SemanticSugar/traildb-haskell. Then follow these instructions.

You need at least `traildb`, `Judy` and `cmph` libraries installed to compile
and use these bindings. The latter two are dependencies of TrailDB itself so if
you have TrailDB the C library installed properly, then most likely you don't
need to do anything else regarding dependencies.

### cabal-install

Cabal is usually in the package repositories of your distribution.

    $ apt-get install cabal-install      # Debian/Mint/Ubuntu
    $ dnf install ghc cabal-install      # Fedora 22
    $ pacman -S ghc cabal-install        # Arch Linux
    $ pkg install hs-cabal-install       # FreeBSD

    $ cabal install                      # Run this in the root of traildb-haskell

After this, bindings should be usable in other Haskell projects by requiring `traildb`.

### stack

Stack is a new fancy Haskell build tool. Because it's new and fancy, it doesn't
quite have the same level of presence in Linux package repositories than
`cabal-install`.

You can manually download `stack` from [http://www.haskellstack.org/] if it's
not in your repositories.

    # Once you have `stack` in your PATH:

    $ stack setup      # May be optional if you have GHC already installed and it can be used
    $ stack install

License
-------

These bindings are licensed under the MIT license.

How to contribute or report bugs
--------------------------------

You can use our [GitHub page](https://github.com/SemanticSugar/traildb-haskell/) to report issues or pull requests. 

