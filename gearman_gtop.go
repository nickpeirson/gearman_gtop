package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/nsf/termbox-go"
	"github.com/pmylund/sortutil"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var VERSION = "0.1.0"

type statusLine struct {
	name    string
	queued  string
	running string
	workers string
}

type fieldWidths struct {
	name    int
	queued  int
	running int
	workers int
}

type gearmanStatus struct {
	statusLines    []statusLine
	statusLineDims fieldWidths
}

type sortType struct {
	field     string
	ascending bool
}

var columnNames = statusLine{"Job name", "Queued", "Running", "Workers"}
var sortFields = []string{"name", "queued", "running", "workers"}
var sortOrder = sortType{}

type byQueued []statusLine

func (a byQueued) Len() int      { return len(a) }
func (a byQueued) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byQueued) Less(i, j int) bool {
	inti, _ := strconv.Atoi(a[i].queued)
	intj, _ := strconv.Atoi(a[j].queued)
	return inti < intj
}

type byRunning []statusLine

func (a byRunning) Len() int      { return len(a) }
func (a byRunning) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byRunning) Less(i, j int) bool {
	inti, _ := strconv.Atoi(a[i].running)
	intj, _ := strconv.Atoi(a[j].running)
	return inti < intj
}

type byWorkers []statusLine

func (a byWorkers) Len() int      { return len(a) }
func (a byWorkers) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byWorkers) Less(i, j int) bool {
	inti, _ := strconv.Atoi(a[i].workers)
	intj, _ := strconv.Atoi(a[j].workers)
	return inti < intj
}

func fieldWidthsFactory(line statusLine) fieldWidths {
	return fieldWidths{
		len(line.name),
		len(line.queued),
		len(line.running),
		len(line.workers)}
}

func fatal(msg string) {
	termbox.Close()
	log.Println("Exiting: ", msg)
	fmt.Println(msg)
	os.Exit(2)
}

func statusLineFromString(line string) (statusLine, error) {
	parts := strings.Fields(line)
	if len(parts) != 4 {
		return statusLine{}, errors.New("Wrong number of fields")
	}
	return statusLine{parts[0], parts[1], parts[2], parts[3]}, nil
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func initialiseFilters() (include, exclude []string){
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

func includeLine(line statusLine, includeTerms, excludeTerms []string) bool {
	if len(includeTerms) == 0 && len(excludeTerms) == 0 {
		return true
	}
	name := strings.ToLower(line.name)
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
	if len(includeTerms) == 0 {
		return true
	}
	return false
}

func getStatus(c chan gearmanStatus) {
	log.Println("Connecting to gearman")
	const waitTime = 1000 * time.Millisecond
	gearman, err := net.DialTimeout("tcp", gearmanHost+":"+gearmanPort, 1*time.Second)
	if err != nil {
		fatal("Couldn't connect to gearman on " + gearmanHost + ":" + gearmanPort)
		return
	}
	defer gearman.Close()
	gearmanStream := bufio.NewReader(gearman)
	includeTerms, excludeTerms := initialiseFilters()
	for {
		log.Println("Getting status")
		start := time.Now()
		widths := fieldWidthsFactory(columnNames)
		gearman.Write([]byte("status\n"))
		statusLines := make([]statusLine, 0)
		for {
			line, err := gearmanStream.ReadString('\n')
			if err != nil {
				break
			}
			if line == ".\n" {
				break
			}
			statusLine, ok := statusLineFromString(line)
			if ok != nil {
				continue
			}
			if !showAll && statusLine.queued == "0" &&
				statusLine.running == "0" && statusLine.workers == "0" {
				continue
			}
			if !includeLine(statusLine, includeTerms, excludeTerms) {
				continue
			}
			widths.name = max(len(statusLine.name), widths.name)
			widths.queued = max(len(statusLine.queued), widths.queued)
			widths.running = max(len(statusLine.running), widths.running)
			widths.workers = max(len(statusLine.workers), widths.workers)
			statusLines = append(statusLines, statusLine)
		}
		c <- gearmanStatus{statusLines, widths}
		duration := time.Since(start)
		time.Sleep(waitTime - duration)
	}
}

func print_tb(x, y int, fg, bg termbox.Attribute, msg string) {
	for _, c := range msg {
		termbox.SetCell(x, y, c, fg, bg)
		x++
	}
}

func printf_tb(x, y int, fg, bg termbox.Attribute, format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	print_tb(x, y, fg, bg, s)
}

func printField(x, y, fieldWidth int, value string, bold bool) int {
	intValue, ok := strconv.Atoi(value)
	if ok == nil {
		value = fmt.Sprintf("%"+strconv.Itoa(fieldWidth)+"d", intValue) + " "
	}
	fg := termbox.ColorDefault
	if bold {
		fg |= termbox.AttrBold
	}
	print_tb(x, y, fg, termbox.ColorDefault, value)
	return x + fieldWidth
}

func printLine(y int, widths fieldWidths, line statusLine, bold bool) {
	x := 0
	if len(line.name) > widths.name {
		line.name = line.name[:widths.name]
	}
	x = printField(x, y, widths.name+1, line.name, bold)
	x = printField(x, y, widths.queued+1, line.queued, bold)
	x = printField(x, y, widths.running+1, line.running, bold)
	x = printField(x, y, widths.workers, line.workers, bold)
}

func sortStatusLines(gearmanStatus *gearmanStatus) {
	switch sortOrder.field {
	case "name":
		if sortOrder.ascending {
			sortutil.CiAscByField(gearmanStatus.statusLines, sortOrder.field)
		} else {
			sortutil.CiDescByField(gearmanStatus.statusLines, sortOrder.field)
		}
	case "queued":
		if sortOrder.ascending {
			sort.Sort(byQueued(gearmanStatus.statusLines))
		} else {
			sortutil.SortReverseInterface(byQueued(gearmanStatus.statusLines))
		}
	case "running":
		if sortOrder.ascending {
			sort.Sort(byRunning(gearmanStatus.statusLines))
		} else {
			sortutil.SortReverseInterface(byRunning(gearmanStatus.statusLines))
		}
	case "workers":
		if sortOrder.ascending {
			sort.Sort(byWorkers(gearmanStatus.statusLines))
		} else {
			sortutil.SortReverseInterface(byWorkers(gearmanStatus.statusLines))
		}
	}
}

func drawStatusLine(gearmanStatus gearmanStatus, position, y, width int) {
	progress := fmt.Sprintf("%d/%d", position, len(gearmanStatus.statusLines))
	print_tb(width - len(progress), y, termbox.ColorDefault, termbox.ColorDefault, progress)
}

func drawStatus(gearmanStatus gearmanStatus, position, height, width int) {
	sortStatusLines(&gearmanStatus)
	lines := gearmanStatus.statusLines
	log.Print("First line: ", lines[0])
	log.Print("Last line: ", lines[len(lines) - 1])
	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
	y := 0
	printY := 0

	widths := gearmanStatus.statusLineDims
	totalWidth := widths.name + widths.queued + widths.running + widths.workers + 3
	if totalWidth > width {
		widths.name += width - totalWidth
	}
	printLine(0, widths, columnNames, true)
	printY = y - position + 1
	for _, line := range lines {
		if printY > height {
			break
		}
		if printY < 1 {
			printY++
			continue
		}
		printLine(printY, widths, line, false)
		printY++
	}
	//drawStatusLine(gearmanStatus, position, printY, width)
	termbox.Flush()
}

func sortEvent(index rune) {
	sortIndex, _ := strconv.Atoi(string(index))
	sortField := sortFields[sortIndex-1]
	if sortOrder.field == sortField {
		sortOrder.ascending = !sortOrder.ascending
	} else if sortIndex == 1 {
		sortOrder.ascending = true
	} else {
		sortOrder.ascending = false
	}
	sortOrder.field = sortField
}

func handleEvents(direction chan int, resized chan termbox.Event, doRedraw chan bool, quit chan bool) {
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
					direction <- -1
				case termbox.KeyArrowDown:
					direction <- 1
				}
			}
		case termbox.EventResize:
			resized <- event
		}
	}
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

func getDisplayArea() (width, height int) {
	width, height = termbox.Size()
	height--
	return
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

func initLogging() *os.File {
	f, err := os.OpenFile("/tmp/gearman_gtop.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	return f
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
	flag.StringVar(&queueNameInclude, "include", "", "Include queues containing this string")
	flag.StringVar(&queueNameExclude, "exclude", "", "Exclude queues containing this string")
}

func redraw(currentGearmanStatus gearmanStatus, position int) {
	width, height := getDisplayArea()
	drawStatus(currentGearmanStatus, position, height, width)
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
	status := make(chan gearmanStatus)
	doRedraw := make(chan bool)
	quit := make(chan bool)
	resized := make(chan termbox.Event)
	scroll := make(chan int, 3)

	err := termbox.Init()
	if err != nil {
		fatal(err.Error())
	}
	defer termbox.Close()
	termbox.SetInputMode(termbox.InputEsc)

	go getStatus(status)
	go handleEvents(scroll, resized, doRedraw, quit)
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
