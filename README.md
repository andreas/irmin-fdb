# `irmin-fdb`

`irmin-fdb` implements an [Irmin](https://github.com/mirage/irmin) store backed by [FoundationDB](https://foundationdb.org). The implementation is based on [`ocaml-fdb`](https://github.com/andreas/ocaml-fdb).

See the [Irmin tutorial](https://irmin.io/tutorial/backend) for more information onwriting storage backends for Irmin.

## Requirements

Installing `libfdb` ([download here](https://www.foundationdb.org/download/)) is a requirement for using `irmin-fdb`.

## Install

This package is not available on OPAM yet, but in the meantime you can install it in the following manner:

```
opam pin add irmin-fdb git://github.com/andreas/irmin-fdb.git
```

## Build

```
dune build
```

## Test

```
dune runtest
```
