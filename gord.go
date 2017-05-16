/*
Copyright 2017 Jeff Forristal - www.forristal.com

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Go Resumable Downloader (GORD) is meant for scenarios involving long-running 
// HTTP GET streaming downloads, where it's preferred to resume the download
// stream transparently when encountering a network hiccup.  A Reader can safely
// be used concurrently (although operations internally are serialized).  The
// default timeouts are intentionally generous, since the purpose is to try to
// resume long-running/expensive downloads despite occasional periods of total
// network disruption.
package gord

// BUG(jf): The HTTP response code (in error situation) is not currently returned/visible to the parent.
// BUG(jf): Missing ability to set an HTTP header/cookie, for auth types other than HTTP Basic.

import (
	"io"
	"net"
	//"fmt"
	"time"
	"sync"
	"errors"
	"strconv"
	"net/http"
	"sync/atomic"
)

const (
	maxReadRetries = 5
	maxConnRetries = 3
	idleTimeout = time.Duration(90) * time.Second
	generalTimeout = time.Duration(30) * time.Second
)

var (
	// Error returned when encoutering an unexpected HTTP response code
	ErrResponseStatusCode = errors.New("Unexpected HTTP response code")

	// Error returned when failing to make a successful HTTP request (timeout, etc.)
	ErrResponseFailure = errors.New("Unable to make successful HTTP request")

	StatsRequestTimeouts uint64
	StatsRequestErrors uint64
	StatsRequestOK uint64
	StatsResponseOK uint64
	StatsResponseBadStatus uint64
	StatsReadInterrupted uint64
	StatsReadTimeouts uint64
	StatsReadErrors uint64
	StatsReadOK uint64
)

type Config struct {

	// HTTP Basic Auth user name (also requires BasicAuthPass be specified)
	BasicAuthUser 	*string

	// HTTP Basic Auth password (also requires BasicAuthUser be specified)
	BasicAuthPass 	*string
}

type Reader struct {
	done 		bool
	url 		string
	client 		*http.Client
	response 	*http.Response
	offset 		uint64
	config 		*Config
	lock 		*sync.Mutex
}

// Create a new Reader for the specified url
func NewReader(url string) *Reader {

	transport := &http.Transport {
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: generalTimeout,
			KeepAlive: idleTimeout,
			DualStack: true,
		}).DialContext,
		MaxIdleConns: 2,
		TLSHandshakeTimeout: generalTimeout,
		IdleConnTimeout: idleTimeout,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: generalTimeout,
		DisableKeepAlives: true,
	}

	client := &http.Client {
		Transport: transport,
	}

	return NewReaderConfig(url, client, &Config{})
}

// Create a new Reader for the specified url, allowing to provide a custom 
// configured HTTP client (e.g. set custom timeouts, etc.; required) and 
// additional feature Config (optional, can be nil)
func NewReaderConfig(url string, client *http.Client, config *Config) *Reader {
	if client == nil {
		panic("http.Client is required")
	}
	if config == nil {
		config = &Config{}
	}
	return &Reader{false, url, client, nil, 0, config, &sync.Mutex{}}
}

func (r *Reader) resumeWithLock() error {
	// "WithLock" means the parent is responsible for holding a
	// mutex over r

	if r.response != nil {
		// There is already an active response, just use it
		return nil
	}

	// Create a new request
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	req.Close = true

	// Check if we need to resume at an offset
	if r.offset > 0 {
		rng := "bytes=" + strconv.FormatUint(r.offset, 10) + "-"
		//fmt.Println("- Resuming to ", rng)
		req.Header.Add("Range", rng)
	}

	// Support basic auth
	if r.config.BasicAuthUser != nil && r.config.BasicAuthPass != nil {
		req.SetBasicAuth(*r.config.BasicAuthUser, *r.config.BasicAuthPass)
	}

	// TODO: support an arbitrary header

	// Make the request
	for retries := maxConnRetries; retries > 0; retries-- {
		//fmt.Println("- Making request")
		r.response, err = r.client.Do(req)
		if err != nil {

			// Timeouts are subject to retry
			if err, ok := err.(net.Error); ok && err.Timeout() {
				atomic.AddUint64(&StatsRequestTimeouts, 1)
				continue;
			}

			// We will consider all other errors fatal, and pass them thru
			atomic.AddUint64(&StatsRequestErrors, 1)
			return err
		}

		atomic.AddUint64(&StatsRequestOK, 1)

		// Check we got the right response for initial vs resumed
		if ((r.offset == 0 && r.response.StatusCode == http.StatusOK) ||
			(r.offset > 0 && r.response.StatusCode == http.StatusPartialContent)) {

			// Everything looks good, use this response
			atomic.AddUint64(&StatsResponseOK, 1)
			return nil
		}

		//fmt.Printf("CODE: %d", r.response.StatusCode)

		// Unexpected error, we can't use this response anymore
		// TODO: expose the response code, so caller can decide to retry?
		atomic.AddUint64(&StatsResponseBadStatus, 1)
		r.response.Body.Close()
		r.response = nil
		return ErrResponseStatusCode
	}

	// Exhausted retries, we're done
	return ErrResponseFailure
}

// General Reader Read() function, which transparently will resume interrupted 
// connections under the hood.  An io.EOF error is returned at the point when
// the server indicates the download stream is done. Otherwise, 
// io.ErrUnexpectedEOF indicates an interruption that could not be immediately 
// recovered, by may be recoverable upon next Read().  Other errors should 
// generally be considered fatal.
func (r *Reader) Read(p []byte) (n int, err error) {

	// Check if we've previously completed
	if r.done {
		// NOTE: we may be done for other error reasons, but the
		// correct error will be reported upon the error happening,
		// and everything after that will simply be EOF:
		return 0, io.EOF
	}

	// Synchronize concurrent access
	r.lock.Lock()
	defer r.lock.Unlock()

	// Our main retry loop
	for retries := maxReadRetries; retries > 0; retries-- {

		// Ensure we have a working connection
		err := r.resumeWithLock()
		if err != nil {
			return 0, err
		}

		// Now just read from the response body
		n, err := r.response.Body.Read(p)
		if err != nil {
			// Error occurred, the response body is dead to us now
			r.response.Body.Close()
			r.response = nil

			// EOF is graceful ending
			if err == io.EOF {
				r.done = true
				return 0, err
			}

			// Unexpected EOF is subject to retry
			if err == io.ErrUnexpectedEOF {
				atomic.AddUint64(&StatsReadInterrupted, 1)
				continue
			}

			// Timeouts are subject to retry
			if err, ok := err.(net.Error); ok && err.Timeout() {
				atomic.AddUint64(&StatsReadTimeouts, 1)
				continue;
			}

			// We translate all other errors into unexpected EOF, later
			atomic.AddUint64(&StatsReadErrors, 1)
			r.done = true
			break
		}

		atomic.AddUint64(&StatsReadOK, 1)

		// Update our read offset, for future Range resumes
		r.offset += uint64(n)

		// All set
		return n, nil
	}

	// Ran out of retries, or hit a fatal error -- act as unexpected EOF
	return 0, io.ErrUnexpectedEOF
}


// The Close() function can be used to ensure resource cleanup of the Reader
func (r *Reader) Close() {

	// If we're already done, there is nothing to do
	if r.done {
		return
	}

	// Synchronize concurrent access
	r.lock.Lock()
	defer r.lock.Unlock()

	// Close out any open responses
	if r.response != nil {
		r.response.Body.Close()
		r.response = nil
	}
	r.done = true
}
