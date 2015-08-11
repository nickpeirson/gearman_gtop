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

type display struct {
	statusLines   gearadmin.StatusLines
	fieldWidths   fieldWidths
	position      int
	width         int
	height        int
	headerHeight  int
	footerHeight  int
	numberOfRows  int
	sortField     rune
	sortAscending bool
	redraw        chan bool
}

type fieldWidths struct {
	name    int
	queued  int
	running int
	workers int
	total   int
}

var pollInterval = 1 * time.Second
var quit = make(chan bool)
var statusDisplay = display{}
var columnNames = gearadmin.StatusLine{
	Name:    "Job name",
	Queued:  "Queued",
	Running: "Running",
	Workers: "Workers",
}

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
	hostDefault := "localhost:4730"
	hostUsage := "Gearmand host to connect to. Specify multiple separated by ';'"
	flag.StringVar(&gearmanHost, "host", hostDefault, hostUsage)
	flag.StringVar(&gearmanHost, "h", hostDefault, hostUsage+" (shorthand)")
	flag.StringVar(&initialSortIndex, "sort", "1", "Index of the column to sort by")
	flag.StringVar(&queueNameInclude, "filterInclude", "", "Include queues containing this string. Can provide multiple separated by commas.")
	flag.StringVar(&queueNameExclude, "filterExclude", "", "Exclude queues containing this string. Can provide multiple separated by commas.")
	statusDisplay.redraw = make(chan bool, 5)
}

func main() {
	flag.Parse()
	if doLogging {
		defer (initLogging()).Close()
	} else {
		log.SetOutput(ioutil.Discard)
	}
	statusDisplay.sortEvent(rune(initialSortIndex[0]))

	err := termbox.Init()
	if err != nil {
		fatal(err.Error())
	}
	defer termbox.Close()
	termbox.SetInputMode(termbox.InputEsc)
	log.Println("Termbox initialised")

	statusDisplay.resize(termbox.Size())

	go statusDisplay.updateLines()
	go handleEvents()
	go statusDisplay.draw()
	<-quit
	log.Println("Exiting")
	return
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
				statusDisplay.sortEvent(event.Ch)
			default:
				switch event.Key {
				case termbox.KeyCtrlC:
					quit <- true
				case termbox.KeyArrowUp:
					statusDisplay.scrollOutput(-1)
				case termbox.KeyArrowDown:
					statusDisplay.scrollOutput(+1)
				}
			}
		case termbox.EventResize:
			log.Println("Redrawing for resize")
			statusDisplay.resize(event.Width, event.Height)
		}
	}
}

func (d *display) updateLines() {
	log.Println("Connecting to gearman")
	connectionDetails := strings.Split(gearmanHost, ";")
	var clients []gearadmin.Client
	for _, connectionDetail := range connectionDetails {
		splitConnectionDetail := strings.Split(connectionDetail, ":")
		if len(splitConnectionDetail) > 2 {
			fatal("Invalid connection string: " + connectionDetail)
			return
		}
		host := splitConnectionDetail[0]
		port := "4730"
		if len(splitConnectionDetail) == 2 {
			port = splitConnectionDetail[1]
		}
		gearadminClient := gearadmin.New(host, port)
		defer gearadminClient.Close()
		clients = append(clients, gearadminClient)
	}
	responseFilter := statusFilter(initialiseFilters())
	for {
		log.Println("Getting status")
		start := time.Now()
		statusLines := gearadmin.StatusLines{}
		for _, client := range clients {
			newStatusLines, err := client.StatusFiltered(responseFilter)
			if err != nil {
				fatal("Couldn't get gearman status from " + client.ConnectionString() + " (Error: " + err.Error() + ")")
				return
			}
			statusLines = statusLines.Merge(newStatusLines)
		}
		d.statusLines = statusLines
		d.sortLines()
		d.fieldWidths = fieldWidthsFactory(statusLines)
		d.redraw <- true
		duration := time.Since(start)
		time.Sleep(pollInterval - duration)
	}
}

func (d *display) scrollOutput(direction int) {
	log.Println("Scrolling")
	scrolledToTop := d.position == 0
	scrolledToBottom := len(d.statusLines)-d.position <= d.numberOfRows
	if (direction < 0 && !scrolledToTop) || (direction > 0 && !scrolledToBottom) {
		log.Println("Moving")
		d.position += direction
		d.redraw <- true
	}
}

func (d *display) draw() {
	for {
		<-d.redraw
		lines := d.statusLines

		widths := d.fieldWidths
		widths.name += d.width - widths.total

		termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
		if len(lines) > 0 {
			log.Print("First line: ", lines[0])
			log.Print("Last line: ", lines[len(lines)-1])
		} else {
			log.Print("No lines")
		}
		d.headerHeight = drawHeader(widths)
		d.footerHeight = drawFooter(lines, d.position, d.height, d.width)
		d.numberOfRows = d.height - d.headerHeight - d.footerHeight
		printY := d.headerHeight
		printLines := lines[d.position:]
		if len(printLines) > d.numberOfRows {
			printLines = printLines[:d.numberOfRows]
		}
		for _, line := range printLines {
			drawLine(printY, widths, line, false)
			printY++
		}

		termbox.Flush()
	}
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

func drawFooter(sl gearadmin.StatusLines, position, y, width int) (footerHeight int) {
	footerHeight = 1
	displayedLines := y + position - 1
	totalLines := len(sl)
	progress := fmt.Sprintf("%d/%d", min(displayedLines, totalLines), totalLines)
	print_tb(width-len(progress), y-footerHeight, termbox.ColorDefault, termbox.ColorDefault, progress)
	return
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

var sortFields = map[rune]gearadmin.By{
	'1': gearadmin.ByName,
	'2': gearadmin.ByQueued,
	'3': gearadmin.ByRunning,
	'4': gearadmin.ByWorkers,
}

func (d *display) sortLines() {
	d.statusLines.Sort(sortFields[d.sortField], d.sortAscending)
}

func (d *display) sortEvent(index rune) {
	log.Println("Handling sort event")
	if d.sortField == index {
		d.sortAscending = !d.sortAscending
	} else if index == '1' {
		d.sortAscending = true
	} else {
		d.sortAscending = false
	}
	d.sortField = index
	d.sortLines()
	log.Printf("%#v\n", d.redraw)
	d.redraw <- true
}

func (d *display) resize(width, height int) {
	log.Println("Display resized")
	d.height = height
	d.width = width
	d.redraw <- true
}

func initLogging() *os.File {
	f, err := os.OpenFile("/tmp/gearman_gtop.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	log.Println("Logging initialised")
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
