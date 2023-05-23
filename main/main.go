package main

import (
	"log"
	"net/url"

	"github.com/zhoukk/krtspd"
)

func main() {
	rs := krtspd.NewRtspServer(":8554", 300)
	rs.SetHook(krtspd.RtspdHook{
		PushStart: func(s string, u *url.URL) bool {
			log.Printf("%s push %s start\n", s, u.String())
			return true
		},
		PushEnd: func(s string, u *url.URL) {
			log.Printf("%s push %s end\n", s, u.String())
		},
		PullStart: func(s string, u *url.URL) bool {
			log.Printf("%s pull %s start\n", s, u.String())
			return true
		},
		PullEnd: func(s string, u *url.URL) {
			log.Printf("%s pull %s end\n", s, u.String())
		},
	})
	rs.Start()
}
