[![Build Status](https://travis-ci.org/alephcloud/hs-aws-performance-tests.svg?branch=master)](https://travis-ci.org/alephcloud/hs-aws-performance-tests)

**By using the dynamo-performace application from this package with your AWS API
credentials costs will incure to your AWS account. Depending on the provisioned
test table read and write throughput these costs can be in the order of several
dollars per hour.**

**Also be aware that there is an option to keep the table after the tests are finished
(for example for usage with successive test runs). If you use that option you have to
make sure that you delete the table yourself when you don't need it any more.**

Installation
============

First make sure that you have at least version 1.20 of *Cabal*. You can upgrade
to a recent version via

~~~{.bash}
cabal install Cabal --constraint='cabal>=1.20'
~~~

It is important that you upgrade to a recent *Cabal* version *before* you
run one of the following commands.

For installation from [Hackage](http://hackage.haskell.org/package/aws-performance-tests)
you run:

~~~{.bash}
cabal install aws-performance-tests
~~~

For installation from the [GitHub repository](https://github.com/alephcloud/hs-aws-performance-tests)
can can use the following commands:

~~~{.bash}
git clone https://github.com/alephcloud/hs-aws-performance-tests
cd hs-aws-performance-tests
cabal install
~~~

Charts
------

Optionally the application can be compiled with support for generating charts. For
this you must install *cairo* and *gtk2hs-buildtools*. You also need recent versions
of *alex* and *happy*. On a Linux/Debian system you can install *cairo* as follows:

~~~{.bash}
sudo apt-get install libcairo2
~~~

The Haskell build-tools are installed via:

~~~
cabal install alex happy gtk2hs-buildtools
~~~

You then pass the flag `-fwith-chart` to the the installation commands:

~~~{.bash}
cabal install aws-performance-tests -fwith-chart
~~~

Or when compiling the sources from GitHub:

~~~{.bash}
git clone https://github.com/alephcloud/hs-aws-performance-tests
cd hs-aws-performance-tests
cabal install -fwith-chart
~~~

Usage
=====

After installing the package you'll find the executable `dynamodb-performance` in the
default location where cabal is configured to install binaries.

In order to use the application you must put your AWS API credentials for
your AWS account in the file `~/.aws-keys` as described in the
[Documentation of the aws package](https://github.com/aristidb/aws#example-usage).

For help on available options you may call the executable with `--help`:

~~~{.bash}
dynamodb-performance --help
~~~

