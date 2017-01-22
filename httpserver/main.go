package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("--------------------------")
		log.Printf("remote addr: %#v\n", r.RemoteAddr)
		log.Printf("header: %#v\n", r.Header)
	})
	http.ListenAndServe(":80", nil)
}
