# query-txn-hbase

Runs an HBase query against account-txns table to extract any columns
that are named MSG_TIMESTAMP-9999 (where 9999 is a transaction id).

Writes a csv file for every column found keyed by the transaction id
that contains the value of the MSG_TIMESTAMP column (a Long - time in
ms) and the timestamp from HBase that the cell was writen.

## Installation

Download from http://example.com/FIXME.

## Usage

You can uberjar (run 'lein uberjar' from the root of the project) and
run this application like so.

    $ java -jar query-txn-hbase-0.1.0-standalone.jar

The resources/config.edn file specifies the HBase config parameters. I
you wish to override these for development or test look at the
profiles-template.clj file, rename it to profiles.clj and edit it to
taste.

When you run as a profile the appropriate environment (:env map) from
profiles.clj file will be used. By default 'lein run' will use :dev.

## Options

None TODO add optional output file name.

## Examples

...

### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

## License

Copyright © 2015 Chris Howe-Jones

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
