package utils

import (
	log "github.com/en/siren/utils/glog"
)

func SirenRecover() {
	if r := recover(); r != nil {
		log.Debug().Interface("recover", r).Msg("")
	}
}
