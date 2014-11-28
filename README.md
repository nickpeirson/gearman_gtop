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
  -a=false: Show all queues, even if the have no workers or jobs (shorthand)
  -all=false: Show all queues, even if the have no workers or jobs
  -filterExclude="": Exclude queues containing this string. Can provide multiple separated by commas.
  -filterInclude="": Include queues containing this string. Can provide multiple separated by commas.
  -h="localhost": Gearmand host to connect to (shorthand)
  -host="localhost": Gearmand host to connect to
  -l=false: Log debug to /tmp/gearman_gtop.log (shorthand)
  -log=false: Log debug to /tmp/gearman_gtop.log
  -p="4730": Gearmand port to connect to (shorthand)
  -port="4730": Gearmand port to connect to
  -sort="1": Index of the column to sort by
```
### Runtime
* Pressing `1` through `4` will sort by the relavent column
* `q` to quit