package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type pidController struct {
	mu sync.Mutex

	kp, ki, kd float64
	setpoint   float64
	integral   float64
	prevError  float64
	lastTime   time.Time

	computeOutput func() float64
	setControl    func(float64)
}

func newPidController(kp, ki, kd, setpoint float64, computeOutput func() float64, setControl func(float64)) *pidController {
	return &pidController{
		kp:            kp,
		ki:            ki,
		kd:            kd,
		setpoint:      setpoint,
		lastTime:      time.Now(),
		computeOutput: computeOutput,
		setControl:    setControl,
	}
}

func (c *pidController) run(s *stats) {
	for {
		c.mu.Lock()

		now := time.Now()
		dt := now.Sub(c.lastTime).Seconds()
		c.lastTime = now

		output := c.computeOutput()
		error := c.setpoint - output
		c.integral += error * dt
		derivative := (error - c.prevError) / dt
		c.prevError = error

		s.emit("pid_error", error)
		s.emit("pid_integral", c.integral)
		s.emit("pid_derivative", derivative)

		control := c.kp*error + c.ki*c.integral + c.kd*derivative

		s.emit("pid_output", control)

		c.setControl(control)

		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

var start time.Time = time.Now()

func elapsed() float64 {
	return time.Since(start).Seconds()
}

type stats struct {
	mu sync.Mutex

	stats map[string]float64
	names []string
	funcs []func(dt float64) float64
}

func newStats() *stats {
	return &stats{
		stats: make(map[string]float64),
	}
}

func (s *stats) run() {
	last := elapsed()
	for {
		time.Sleep(10 * time.Millisecond)
		s.mu.Lock()
		now := elapsed()
		s.stats["time"] = now
		for i, f := range s.funcs {
			s.stats[s.names[i]] = f(now - last)
		}
		last = now
		statsJson, err := json.Marshal(s.stats)
		if err != nil {
			panic("failed to marshal stats")
		} else {
			fmt.Println(string(statsJson))
		}
		s.mu.Unlock()
	}
}

func (s *stats) register(name string, f func(dt float64) float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names = append(s.names, name)
	s.funcs = append(s.funcs, f)
}

func (s *stats) emit(name string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats[name] = value
}

func main() {
	s := newStats()

	d := newDb(config{
		maxMemtableSize: 100,
	}, s)

	go d.runFlusher()
	go d.runCompactor()
	go s.run()
	s.register("health", func(dt float64) float64 {
		return d.health()
	})
	s.register("target_health", func(dt float64) float64 {
		return 1
	})
	s.register("throttle", func(dt float64) float64 {
		return float64(d.throttle)
	})
	s.register("writes_per_second", func(dt float64) float64 {
		// i think this is racy lol
		w := d.writes
		d.writes = 0
		return float64(w) / dt
	})

	pid := newPidController(1, 0.1, 0.00, 1, d.health, func(control float64) {
		d.throttle = -time.Second * time.Duration(control)
	})
	go pid.run(s)

	const writesPerSecond = 5000
	for elapsed() < 10.0 {
		d.write(1)
		time.Sleep(time.Second / writesPerSecond)
	}
	d.close()
}

func (d *db) close() {
	close(d.flushJobs)
	close(d.compactionJobs)
}

type config struct {
	maxMemtableSize int
}

type flushJob struct {
	size int
}

type compactionJob struct {
	ssts        []sst
	targetLevel int
}

type db struct {
	mu sync.Mutex

	config       config
	memtableSize int
	// drained at each stats flush
	writes   int
	levels   [][]sst
	nextId   int
	stats    *stats
	throttle time.Duration

	flushJobs      chan flushJob
	compactionJobs chan compactionJob
}

func newDb(c config, s *stats) *db {
	return &db{
		config:         c,
		memtableSize:   0,
		levels:         make([][]sst, 1),
		nextId:         0,
		flushJobs:      make(chan flushJob, 100),
		compactionJobs: make(chan compactionJob, 100),
		stats:          s,
	}
}

const maxSstsInLevel = 5

// Assumes mu is held.
func (d *db) installSst(prevSsts []sst, s sst, level int) {
	for len(d.levels) <= level {
		d.levels = append(d.levels, nil)
	}
	oldIds := make(map[int]struct{})
	for _, s := range prevSsts {
		oldIds[s.id] = struct{}{}
	}
	d.levels[level] = append(d.levels[level], s)
	// Go through each level and remove the old SSTs.
	for i := range d.levels {
		var newSsts []sst
		for _, s := range d.levels[i] {
			if _, ok := oldIds[s.id]; !ok {
				newSsts = append(newSsts, s)
			}
		}
		d.levels[i] = newSsts
		d.stats.emit(fmt.Sprintf("l%d_size", i), float64(len(d.levels[i])))
	}

	// Now if any level is too big, compact it.
	for i := range d.levels {
		if len(d.levels[i]) > maxSstsInLevel {
			var totalSize int
			for _, s := range d.levels[i] {
				totalSize += s.size
			}
			select {
			case d.compactionJobs <- compactionJob{
				ssts:        d.levels[i],
				targetLevel: i + 1,
			}:
			default:
			}
		}
	}
}

// Assumes mu is held.
func (d *db) health() float64 {
	h := float64(len(d.levels[0])) / float64(maxSstsInLevel)
	for i := 1; i < len(d.levels); i++ {
		if len(d.levels[i]) > maxSstsInLevel {
			h += float64(len(d.levels[i])) / float64(maxSstsInLevel)
		}
	}
	return h
}

func (d *db) runFlusher() {
	for job := range d.flushJobs {
		d.mu.Lock()

		d.nextId++
		d.installSst(nil, sst{
			size: job.size,
			id:   d.nextId,
		}, 0)

		d.stats.emit("l0_size", float64(len(d.levels[0])))

		d.mu.Unlock()
	}
}

func (d *db) runCompactor() {
	for job := range d.compactionJobs {
		d.mu.Lock()

		d.levels[0] = append(d.levels[0], sst{
			size: d.config.maxMemtableSize,
			id:   d.nextId,
		})

		d.mu.Unlock()
		size := 0
		for _, s := range job.ssts {
			size += s.size
		}
		time.Sleep(time.Millisecond * time.Duration(size) / 100)
		// do the compaction
		d.mu.Lock()
		d.nextId++
		id := d.nextId + 1
		d.nextId++
		d.installSst(job.ssts, sst{
			id:   id,
			size: size,
		}, job.targetLevel)
		d.mu.Unlock()
	}
}

func (d *db) write(n int) {
	time.Sleep(d.throttle)
	d.memtableSize += n
	d.writes += 1
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.memtableSize >= d.config.maxMemtableSize {
		size := d.memtableSize
		d.memtableSize = 0
		select {
		case d.flushJobs <- flushJob{
			size: size,
		}:
		default:
		}
	}
	d.stats.emit("memtable_size", float64(d.memtableSize))
}

type sst struct {
	id   int
	size int
}
