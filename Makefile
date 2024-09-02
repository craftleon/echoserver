
LD_FLAGS = "-s -w"

echoserver:
	go build -trimpath -ldflags ${LD_FLAGS} -v -o echoserver ./main.go

.PHONY: echoserver
