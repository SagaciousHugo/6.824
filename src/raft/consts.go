package raft

import "time"

const (
	HEARTBEATTIMEOUT       = 150 * time.Millisecond
	HEARTBEAT              = 100 * time.Millisecond
	ELECTIONTIMEOUT        = 300 * time.Millisecond
	CommandInstallSnapshot = "RaftCommandInstallSnapshot"
)

const (
	StateMachineNewCommitted = iota
	StateMachineInstallSnapshot
)

const (
	STOPED = iota - 1
	FOLLOWER
	CANDIDATE
	LEADER
)

var stateMap = map[int]string{
	STOPED:    "stoped",
	FOLLOWER:  "follower",
	CANDIDATE: "candidate",
	LEADER:    "leader",
}
