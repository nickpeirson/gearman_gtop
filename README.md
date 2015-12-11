# gearman_gtop
_A gearman_top replacement written in go_

## Differences
* Scrolling
* Sorting
* Filtering

## Usage
### Command line options
```
Usage of gearman_gtop:
  -a, --all[=false]: Show all queues, even if the have no workers or jobs
  -e, --exclude="": Exclude queues containing this string. Can provide multiple separated by commas.
  -h, --host="localhost:4730": Gearmand host to connect to. Specify multiple separated by ';'
  -i, --include="": Include queues containing this string. Can provide multiple separated by commas.
  -l, --log[=false]: Log debug to /tmp/gearman_gtop.log
  -r, --regex="": Queries must match this regex. See https://github.com/google/re2/wiki/Syntax for supported syntax.
      --sort="1": Index of the column to sort by
```
### Runtime
* Pressing `1` through `4` will sort by the relavent column
* `q` to quit