package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer listener.Close()

	aof, err := NewAof("database.aof")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer aof.Close()

	aof.Read(processCommand)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}

		go handleConnection(conn, aof)
	}
}

func processCommand(value Value) {
	command := strings.ToUpper(value.array[0].bulk)
	args := value.array[1:]

	if handler, ok := Handlers[command]; ok {
		handler(args)
	} else {
		fmt.Println("Invalid command:", command)
	}
}

func handleConnection(conn net.Conn, aof *Aof) {
	defer conn.Close()
	resp := NewResp(conn)
	writer := NewWriter(conn)

	for {
		value, err := resp.Read()
		if err != nil {
			fmt.Println("Read error:", err)
			return
		}

		if value.typ != "array" || len(value.array) == 0 {
			writer.Write(Value{typ: "error", str: "Invalid request"})
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		if handler, ok := Handlers[command]; ok {
			if command == "SET" || command == "HSET" {
				aof.Write(value)
			}
			writer.Write(handler(args))
		} else {
			writer.Write(Value{typ: "error", str: "Unknown command"})
		}
	}
}
