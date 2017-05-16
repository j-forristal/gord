# GORD
Go Resumable Downloader

GORD provides a Reader interface that is a wrapper on top of http.Client, 
keeping track of Read() position and transparently resuming interrupted
downloads.

### Requirements/Limitations

* GORD only supports GET download requests
* The HTTP server must honor Range requests
* Only HTTP Basic auth is supported; cookie/header token is a future TODO

### Example Usage
The following example code will read lines from a streaming
download.

```
gr := gord.NewReader("http://your/download")
defer gr.Close()
br := bufio.NewReader(gr)

for {
	data, err := br.ReadString(byte(0x0a))
	if err != nil {
		if err == io.EOF {
			// Download complete
			// ...
		}
		// ... handle error
	}
	// ... do something with data
}
```
