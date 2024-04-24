# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.8] - 2024-04-24

### Bugfix
1. Kept Naming Scheme consistent.
2. Fixed typo for conjuction > conjunction

## [0.1.7] - 2024-04-16

### Added
1. It automatically creates and destroys the cursor. No need to use `with` to enter.

### Removed/Deprecated
1. DBConnect Class. It will still exist but new features will not be supported for backwards compatibility of versions. It is replaced by `SimplePgSQL` class. 

## [0.1.6] - 2024-03-04

### Bugfix
1. fixed Breaking of IN/NOT IN usage. 

## [0.1.5] - 2024-02-28

### Added
1. Read now supports BETWEEN, IN, NOT IN clauses.

## [0.1.4] - 2024-02-28

### Bugfix
1. Fix conditional check on inputs.

## [0.1.3] - 2024-02-21

### Bugfix
1. Corrected Dates in `CHANGELOG.md`
2. Check for Write string statements rather than READ statements.

## [0.1.2] - 2024-02-20

### Added
1. Supports taking column if a raw query is passed for the dataframe to have column names or dictonary to have keys.

## [0.1.1] - 2024-02-20

### Bugfix
1. Fixed f-string was split in multiple lines causing it throw error.

## [0.1.0] - 2024-02-20

### Added

Supports Read and Writing to the Database.

#### Read
1. Specific COlumns
2. Aggegrates to Columns
3. Multiple Conditions
4. Order by
5. Limit
6. Returns via List of Lists, Dict, Pandas Dataframe.


#### Write
1. Row wise entry to specific columns

[0.1.0] - 2024-02-20
