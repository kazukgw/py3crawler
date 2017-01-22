package main

import (
	"github.com/elazarl/goproxy"
	"log"
	"net/http"
)

func main() {
	proxy := goproxy.NewProxyHttpServer()
	proxy.Verbose = true
	proxy.OnResponse().DoFunc(func(r *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
		log.Println("-----------------")
		return r
	})
	log.Fatal(http.ListenAndServe(":8080", proxy))
}
