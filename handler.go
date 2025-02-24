package main

import "sync"

type Storage struct {
	simple   map[string]string
	hashes   map[string]map[string]string
	simpleMu sync.RWMutex
	hashMu   sync.RWMutex
}

var store = Storage{
	simple: make(map[string]string),
	hashes: make(map[string]map[string]string),
}

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key, value := args[0].bulk, args[1].bulk

	store.simpleMu.Lock()
	store.simple[key] = value
	store.simpleMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	store.simpleMu.RLock()
	value, exists := store.simple[key]
	store.simpleMu.RUnlock()

	if !exists {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash, key, value := args[0].bulk, args[1].bulk, args[2].bulk

	store.hashMu.Lock()
	if _, ok := store.hashes[hash]; !ok {
		store.hashes[hash] = make(map[string]string)
	}
	store.hashes[hash][key] = value
	store.hashMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash, key := args[0].bulk, args[1].bulk

	store.hashMu.RLock()
	value, exists := store.hashes[hash][key]
	store.hashMu.RUnlock()

	if !exists {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

// Hash Get All operation
func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	store.hashMu.RLock()
	values, exists := store.hashes[hash]
	store.hashMu.RUnlock()

	if !exists {
		return Value{typ: "null"}
	}

	var result []Value
	for k, v := range values {
		result = append(result, Value{typ: "bulk", bulk: k})
		result = append(result, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: result}
}
