package main

import (
	"fmt"
	"os"
	"os/exec"
	"log"
	"strings"
	"errors"
	"io/ioutil"
	"time"
	"math"
	"strconv"
	"flag"
	"github.com/nsf/termbox-go"
)

var columnNames = statusLine{"Job name","Queued","Running","Workers"}

type statusLine struct {
	name string
	queued string
	running string
	workers string
}

type fieldWidths struct {
	name int
	queued int
	running int
	workers int
}

type gearmanStatus struct {
	statusLines []statusLine
	statusLineDims fieldWidths
}

func fieldWidthsFactory(line statusLine) fieldWidths{
	return fieldWidths{
		len(line.name),
		len(line.queued),
		len(line.running),
		len(line.workers)}
}

func statusLineFromString(line string) (statusLine, error) {
	parts := strings.Fields(line)
	if len(parts) != 4 {
		return statusLine{}, errors.New("Wrong number of fields")
	}
	return statusLine{parts[0], parts[1], parts[2], parts[3]}, nil
}

func getStatus(c chan gearmanStatus) {
	const waitTime = 1000 * time.Millisecond
	for {
		log.Println("Getting status")
		start := time.Now()
		widths := fieldWidthsFactory(columnNames)
	    data, err := exec.Command(gearadmin,"--host="+gearmanHost,"--port="+gearmanPort,"--status").Output()
	    if err != nil {
	    	log.Fatal("Couldn't get status from gearadmin: "+err.Error())
	    }
	    strData := string(data)
	    statusLines := make([]statusLine, 0)
	    for _, line := range strings.Split(strData, "\n") {
	    	statusLine, ok := statusLineFromString(line)
	    	if ok != nil {
	    		continue
	    	}
	    	widths.name = int(math.Max(float64(len(statusLine.name)), float64(widths.name)))
	    	widths.queued = int(math.Max(float64(len(statusLine.queued)), float64(widths.queued)))
	    	widths.running = int(math.Max(float64(len(statusLine.running)), float64(widths.running)))
	    	widths.workers = int(math.Max(float64(len(statusLine.workers)), float64(widths.workers)))
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

func printField(x, y, fieldWidth int, value string, bold bool) int{
	intValue, ok := strconv.Atoi(value)
	if ok == nil {
		value = fmt.Sprintf("%"+strconv.Itoa(fieldWidth)+"d",intValue) + " "
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
	x = printField(x, y, widths.name + 1, line.name, bold)
	x = printField(x, y, widths.queued + 1, line.queued, bold)
	x = printField(x, y, widths.running + 1, line.running, bold)
	x = printField(x, y, widths.workers, line.workers, bold)
}

func drawStatus(gearmanStatus gearmanStatus, position int, height int, width int) {
	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
	y := 0
	printY := 0
	
	widths := gearmanStatus.statusLineDims
	totalWidth := widths.name + widths.queued + widths.running + widths.workers + 3;
	if totalWidth > width {
		widths.name += width - totalWidth
	}
	printLine(0, widths, columnNames, true)
	for _, line := range gearmanStatus.statusLines {
		printY = y - position
		if printY > height {
			break
		}
		if printY < 1 {
			y++
			continue
		}
		printLine(printY, widths, line, false)
		y++ 
	}
	termbox.Flush()
}

func handleEvents(direction chan int, resized chan termbox.Event, quit chan bool) {
	for {
		event := termbox.PollEvent()
		log.Println("Recieved event: ",event)
		switch event.Type {
			case termbox.EventKey:
				if event.Ch == 'q' {
					quit<-true
				}
				switch event.Key {
					case termbox.KeyArrowUp:
						direction <- -1
					case termbox.KeyArrowDown:
						direction <- 1
				}
			case termbox.EventResize:
				resized<-event
		}
	}
}

func calculatePosition (currentPosition int, direction int, gearmanStatus gearmanStatus) (int, bool) {
	_, height := termbox.Size()
	scrolledToBottom := len(gearmanStatus.statusLines) <= (currentPosition + height);
	scrolledToTop := currentPosition == 0
	if (direction < 0 && !scrolledToTop) || (direction > 0 && !scrolledToBottom) {
		log.Println("Moving")
		return currentPosition + direction, true
	}
	return currentPosition, false
}

func scrollOutput(direction int, scroll chan int, position int, currentGearmanStatus gearmanStatus) int{
	positionUpdated := false
	log.Println("Scrolling")
	for {
		width, height := termbox.Size()
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

func initLogging() *os.File{
	f, err := os.OpenFile("/tmp/gearman_gtop.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
	    panic(err)
	}
	log.SetOutput(f)
	return f
}

var doLogging bool
var gearadmin string
var gearmanHost string
var gearmanPort string

func init() {
	logDefault := false
	logUsage := "Log debug to /tmp/gearman_gtop.log"
	flag.BoolVar(&doLogging, "log", logDefault, logUsage)
	flag.BoolVar(&doLogging, "l", logDefault, logUsage+" (shorthand)")
	gearadminDefault := "/usr/bin/gearadmin"
	gearadminUsage := "Path to gearadmin, e.g. `which gearadmin`"
	flag.StringVar(&gearadmin, "gearadmin", gearadminDefault, gearadminUsage)
	hostDefault := "localhost"
	hostUsage := "Gearmand host to connect to"
	flag.StringVar(&gearmanHost, "host", hostDefault, hostUsage)
	flag.StringVar(&gearmanHost, "h", hostDefault, hostUsage+" (shorthand)")
	portDefault := "4730"
	portUsage := "Gearmand port to connect to"
	flag.StringVar(&gearmanPort, "port", portDefault, portUsage)
	flag.StringVar(&gearmanPort, "p", portDefault, portUsage+" (shorthand)")
}


func main() {
	
	flag.Parse()
	
	if doLogging {
		defer (initLogging()).Close()
	} else {
		log.SetOutput(ioutil.Discard)
	}
	
	var currentGearmanStatus gearmanStatus
	position := 0
	status := make(chan gearmanStatus)
	quit := make(chan bool)
	resized := make(chan termbox.Event)
	scroll := make(chan int, 3)
	
	err := termbox.Init()
	if err != nil {
		panic(err)
	}
	defer termbox.Close()
	termbox.SetInputMode(termbox.InputEsc)
	
	go getStatus(status)
	go handleEvents(scroll, resized, quit)
	for {
		select {
			case currentGearmanStatus = <-status:
				log.Println("Redrawing for updated status")
				width, height := termbox.Size()
				drawStatus(currentGearmanStatus, position, height, width)
			case ev := <-resized:
				log.Println("Redrawing for resize")
				drawStatus(currentGearmanStatus, position, ev.Height, ev.Width)
			case direction := <-scroll:
				position = scrollOutput(direction, scroll, position, currentGearmanStatus)
			case <-quit:
				log.Println("Exiting")
				return
		}
	}
}
