package main

import (
	"flag"
	"fmt"
	"github.com/nickpeirson/gearadmin"
	"github.com/nsf/termbox-go"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var VERSION = "0.2.0"
var pollInterval = 1 * time.Second
var status = make(chan gearmanStatus)
var doRedraw = make(chan bool)
var quit = make(chan bool)
var resized = make(chan termbox.Event)
var scroll = make(chan int, 3)

type gearmanStatus struct {
	statusLines gearadmin.StatusLines
	fieldWidths fieldWidths
}

type fieldWidths struct {
	name    int
	queued  int
	running int
	workers int
	total   int
}

var columnNames = gearadmin.StatusLine{"Job name", "Queued", "Running", "Workers"}

func fieldWidthsFactory(status gearadmin.StatusLines) (widths fieldWidths) {
	widths = fieldWidths{
		len(columnNames.Name),
		len(columnNames.Queued),
		len(columnNames.Running),
		len(columnNames.Workers),
		0,
	}
	for _, statusLine := range status {
		widths.name = max(len(statusLine.Name)+1, widths.name)
		widths.queued = max(len(statusLine.Queued)+1, widths.queued)
		widths.running = max(len(statusLine.Running)+1, widths.running)
		widths.workers = max(len(statusLine.Workers), widths.workers)
	}
	widths.total = widths.name + widths.queued + widths.running + widths.workers + 3
	return
}

var doLogging bool
var showAll bool
var gearmanHost string
var gearmanPort string
var initialSortIndex string
var queueNameInclude string
var queueNameExclude string

func init() {
	logDefault := false
	logUsage := "Log debug to /tmp/gearman_gtop.log"
	flag.BoolVar(&doLogging, "log", logDefault, logUsage)
	flag.BoolVar(&doLogging, "l", logDefault, logUsage+" (shorthand)")
	allDefault := false
	allUsage := "Show all queues, even if the have no workers or jobs"
	flag.BoolVar(&showAll, "all", allDefault, allUsage)
	flag.BoolVar(&showAll, "a", allDefault, allUsage+" (shorthand)")
	hostDefault := "localhost"
	hostUsage := "Gearmand host to connect to"
	flag.StringVar(&gearmanHost, "host", hostDefault, hostUsage)
	flag.StringVar(&gearmanHost, "h", hostDefault, hostUsage+" (shorthand)")
	portDefault := "4730"
	portUsage := "Gearmand port to connect to"
	flag.StringVar(&gearmanPort, "port", portDefault, portUsage)
	flag.StringVar(&gearmanPort, "p", portDefault, portUsage+" (shorthand)")
	flag.StringVar(&initialSortIndex, "sort", "1", "Index of the column to sort by")
	flag.StringVar(&queueNameInclude, "filterInclude", "", "Include queues containing this string. Can provide multiple separated by commas.")
	flag.StringVar(&queueNameExclude, "filterExclude", "", "Exclude queues containing this string. Can provide multiple separated by commas.")
}

func main() {
	flag.Parse()
	if doLogging {
		defer (initLogging()).Close()
	} else {
		log.SetOutput(ioutil.Discard)
	}
	sortEvent(rune(initialSortIndex[0]))

	var currentGearmanStatus gearmanStatus
	position := 0

	err := termbox.Init()
	if err != nil {
		fatal(err.Error())
	}
	defer termbox.Close()
	termbox.SetInputMode(termbox.InputEsc)

	go getStatus()
	go handleEvents()
	for {
		select {
		case currentGearmanStatus = <-status:
			log.Println("Redrawing for updated status")
			redraw(currentGearmanStatus, position)
		case ev := <-resized:
			log.Println("Redrawing for resize")
			drawStatus(currentGearmanStatus, position, ev.Height, ev.Width)
		case direction := <-scroll:
			position = scrollOutput(direction, scroll, position, currentGearmanStatus)
		case <-doRedraw:
			redraw(currentGearmanStatus, position)
		case <-quit:
			log.Println("Exiting")
			return
		}
	}
}

func getStatus() {
	log.Println("Connecting to gearman")
	gearadminClient := gearadmin.New(gearmanHost, gearmanPort)
	defer gearadminClient.Close()
	responseFilter := statusFilter(initialiseFilters())
	for {
		log.Println("Getting status")
		start := time.Now()
		statusLines, err := gearadminClient.StatusFiltered(responseFilter)
		if err != nil {
			fatal("Couldn't get gearman status from " + gearmanHost + ":" + gearmanPort + " (Error: " + err.Error() + ")")
			return
		}
		status <- gearmanStatus{statusLines, fieldWidthsFactory(statusLines)}
		duration := time.Since(start)
		time.Sleep(pollInterval - duration)
	}
}

func handleEvents() {
	for {
		event := termbox.PollEvent()
		log.Println("Recieved event: ", event)
		switch event.Type {
		case termbox.EventKey:
			switch event.Ch {
			case 'q':
				quit <- true
			case '1', '2', '3', '4':
				sortEvent(event.Ch)
				doRedraw <- true
			default:
				switch event.Key {
				case termbox.KeyCtrlC:
					quit <- true
				case termbox.KeyArrowUp:
					scroll <- -1
				case termbox.KeyArrowDown:
					scroll <- 1
				}
			}
		case termbox.EventResize:
			resized <- event
		}
	}
}

func drawStatus(gearmanStatus gearmanStatus, position, height, width int) {
	gearmanStatus.statusLines.Sort(sortFields[sortOrder.field], sortOrder.ascending)
	lines := gearmanStatus.statusLines

	widths := gearmanStatus.fieldWidths
	widths.name += width - widths.total

	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)

	headerHeight := drawHeader(widths)
	footerHeight := drawFooter(gearmanStatus, position, height, width)
	printY := headerHeight
	printLines := lines[position:]
	if len(printLines) > height-footerHeight {
		printLines = printLines[:height-footerHeight]
	}
	for _, line := range printLines {
		drawLine(printY, widths, line, false)
		printY++
	}

	termbox.Flush()
}

func drawHeader(widths fieldWidths) int {
	drawLine(0, widths, columnNames, true)
	return 1
}

func drawLine(y int, widths fieldWidths, line gearadmin.StatusLine, bold bool) {
	x := 0
	if len(line.Name) > widths.name {
		line.Name = line.Name[:widths.name]
	}
	x = drawField(x, y, widths.name, line.Name, bold)
	x = drawField(x, y, widths.queued, line.Queued, bold)
	x = drawField(x, y, widths.running, line.Running, bold)
	x = drawField(x, y, widths.workers, line.Workers, bold)
}

func drawField(x, y, fieldWidth int, value string, bold bool) int {
	intValue, ok := strconv.Atoi(value)
	if ok == nil {
		value = fmt.Sprintf("%"+strconv.Itoa(fieldWidth)+"d", intValue) + " "
	}
	fg := termbox.ColorDefault
	if bold {
		fg |= termbox.AttrBold
	}
	print_tb(x, y, fg, termbox.ColorDefault, value)
	return x + fieldWidth + 1
}

func drawFooter(gearmanStatus gearmanStatus, position, y, width int) int {
	displayedLines := y + position - 1
	totalLines := len(gearmanStatus.statusLines)
	progress := fmt.Sprintf("%d/%d", min(displayedLines, totalLines), totalLines)
	print_tb(width-len(progress), y, termbox.ColorDefault, termbox.ColorDefault, progress)
	//	progress := fmt.Sprintf("%d/%d", y+position-1, len(gearmanStatus.statusLines))
	//	print_tb(width-len(progress), y, termbox.ColorDefault, termbox.ColorDefault, progress)
	return 1
}

func scrollOutput(direction int, scroll chan int, position int, currentGearmanStatus gearmanStatus) int {
	positionUpdated := false
	log.Println("Scrolling")
	for {
		width, height := getDisplayArea()
		position, positionUpdated = calculatePosition(position, direction, currentGearmanStatus)
		if positionUpdated {
			drawStatus(currentGearmanStatus, position, height, width)
		}
		select {
		case direction = <-scroll:
			log.Println("Collating scrolling")
		default:
			return position
		}
	}
	panic("unreachable")
}

func calculatePosition(currentPosition int, direction int, gearmanStatus gearmanStatus) (int, bool) {
	_, height := getDisplayArea()
	scrolledToBottom := len(gearmanStatus.statusLines) < (currentPosition + height)
	scrolledToTop := currentPosition == 0
	if (direction < 0 && !scrolledToTop) || (direction > 0 && !scrolledToBottom) {
		log.Println("Moving")
		return currentPosition + direction, true
	}
	return currentPosition, false
}

func statusFilter(includeTerms, excludeTerms []string) gearadmin.StatusLineFilter {
	return func(line gearadmin.StatusLine) bool {
		if !showAll && line.Queued == "0" &&
			line.Running == "0" && line.Workers == "0" {
			return false
		}
		if len(includeTerms) == 0 && len(excludeTerms) == 0 {
			return true
		}
		name := strings.ToLower(line.Name)
		for _, excludeTerm := range excludeTerms {
			if strings.Contains(name, excludeTerm) {
				return false
			}
		}
		for _, includeTerm := range includeTerms {
			if strings.Contains(name, includeTerm) {
				return true
			}
		}
		return len(includeTerms) == 0
	}
}

func initialiseFilters() (include, exclude []string) {
	if len(queueNameInclude) > 0 {
		queueNameInclude = strings.ToLower(queueNameInclude)
		include = strings.Split(queueNameInclude, ",")
	}
	if len(queueNameExclude) > 0 {
		queueNameExclude = strings.ToLower(queueNameExclude)
		exclude = strings.Split(queueNameExclude, ",")
	}
	log.Printf("Including: %d %v", len(include), include)
	log.Printf("Excluding: %d %v", len(exclude), exclude)
	return
}

type sortType struct {
	field     rune
	ascending bool
}

var sortFields = map[rune]gearadmin.By{
	'1': gearadmin.ByName,
	'2': gearadmin.ByQueued,
	'3': gearadmin.ByRunning,
	'4': gearadmin.ByWorkers,
}
var sortOrder = sortType{}

func sortEvent(index rune) {
	if sortOrder.field == index {
		sortOrder.ascending = !sortOrder.ascending
	} else if index == '1' {
		sortOrder.ascending = true
	} else {
		sortOrder.ascending = false
	}
	sortOrder.field = index
}

func getDisplayArea() (width, height int) {
	width, height = termbox.Size()
	height--
	return
}

func redraw(currentGearmanStatus gearmanStatus, position int) {
	width, height := getDisplayArea()
	drawStatus(currentGearmanStatus, position, height, width)
}

func initLogging() *os.File {
	f, err := os.OpenFile("/tmp/gearman_gtop.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	return f
}

func print_tb(x, y int, fg, bg termbox.Attribute, msg string) {
	for _, c := range msg {
		termbox.SetCell(x, y, c, fg, bg)
		x++
	}
}

func fatal(msg string) {
	termbox.Close()
	log.Println("Exiting: ", msg)
	fmt.Println(msg)
	os.Exit(2)
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
