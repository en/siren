package main

import (
	"bytes"
	"net/http"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

func writeInnerSlice(w http.ResponseWriter, pb proto.Message) (int, error) {
	m := jsonpb.Marshaler{EmitDefaults: true, OrigName: true}

	var buf bytes.Buffer
	err := m.Marshal(&buf, pb)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return 0, err
	}
	b := buf.Bytes()
	l := bytes.Index(b, []byte("["))
	r := bytes.LastIndex(b, []byte("]"))
	if l == -1 && r == -1 {
		w.WriteHeader(http.StatusInternalServerError)
		return 0, nil
	}

	return w.Write(b[l : r+1])
}

func writeProto(w http.ResponseWriter, pb proto.Message) {
	m := jsonpb.Marshaler{EmitDefaults: true, OrigName: true}
	err := m.Marshal(w, pb)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}
