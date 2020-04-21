package main

import (
	"time"
)

// struct to track items needed to calculate a generic rate (items per second)
type rate struct {
	start time.Time // start time of the rate period
	stop  time.Time // stop time of the rate period
	count int64     // the number of items completed in the period
}

func newRate() rate {
	now := time.Now()

	r := rate{
		start: now,
		stop:  now,
	}

	return r
}

func (r *rate) setStart(t time.Time) {
	r.start = t
}

//func (r *rate) setStartNow() {
//	r.setStart(time.Now())
//}

func (r *rate) setStop(t time.Time) {
	r.stop = t
}

func (r *rate) setStopNow() {
	r.setStop(time.Now())
}

func (r *rate) setCount(n int64) {
	r.count = n
}

func (r *rate) addCount(n int64) {
	r.setCount(r.count + n)
}

func (r *rate) incrementCount() {
	r.addCount(1)
}

func (r *rate) getRate() float64 {
	d := r.stop.Sub(r.start)
	ns := int64(d)

	c := int64(time.Second / time.Nanosecond)
	rt := float64(r.count*c) / float64(ns)

	return rt
}

func (r *rate) getCurrentRate() float64 {
	cr := r
	cr.setStopNow()
	return cr.getRate()
}
