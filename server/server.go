package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"twoPC/twoPC"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var clusters = 3
var clusterSize = 4

type twoPCServer struct {
	twoPC.UnimplementedTwoPCServer
	// Server state
	serverID    int
	clusterID   int
	isLive      bool
	isByzantine bool

	// Database
	DB    *mongo.Collection
	locks *LockMap

	// Consensus stuff
	seq             int
	lastExecuted    int
	currPrimary     int
	log             map[int]*LogEntry
	prePreparedMsgs map[int]*twoPC.PrePrepare
	preparedMsgs    map[int]bool
	committedMsgs   map[int]bool
	executedMsgs    map[int]bool
	datastore       []*twoPC.DSEntry

	// 2PC Stuff
	preparedTxs map[int]bool

	// Misc
	serverConns     []twoPC.TwoPCClient
	clientConn      twoPC.TwoPCClient
	timers          map[int]*time.Timer
	commitsReceived map[int]int
	mu              sync.Mutex
}

type LockMap struct {
	mu    sync.Mutex
	locks map[int]bool
}

type LogEntry struct {
	req           *twoPC.TxRequest
	prePrepareMsg *twoPC.PrePrepare
	prepareMsgs   map[int]*twoPC.Prepare
	commitMsgs    map[int]*twoPC.Commit
	status        string
	twoPCState    string
	senderSeq     int
}

// ---------------MISC. STUFF-------------------

func (s *twoPCServer) acquireLocks(S, R int) {
	if S != -1 {
		for i := 0; i < 1000; i++ {
			fmt.Printf("Attempting to acquire lock for client %d on server %d\n", S, s.serverID)
			s.locks.mu.Lock()
			if s.locks.locks[S] {
				s.locks.mu.Unlock()
				time.Sleep(3 * time.Second)
				continue
			}
			s.locks.locks[S] = true
			s.locks.mu.Unlock()
			fmt.Printf("Successfully acquired lock for client %d on server %d\n", S, s.serverID)
			break
		}
	}
	if R != -1 {
		for j := 0; j < 1000; j++ {
			fmt.Printf("Attempting to acquire lock for client %d on server %d\n", R, s.serverID)
			s.locks.mu.Lock()
			if s.locks.locks[R] {
				s.locks.mu.Unlock()
				time.Sleep(3 * time.Second)
				continue
			}
			s.locks.locks[R] = true
			s.locks.mu.Unlock()
			fmt.Printf("Successfully acquired lock for client %d on server %d\n", R, s.serverID)
			break
		}
	}
}

func (s *twoPCServer) releaseLocks(S, R int) {
	if S != -1 {
		for i := 0; i < 1000; i++ {
			fmt.Printf("Attempting to release lock for client %d on server %d\n", S, s.serverID)
			s.locks.mu.Lock()
			if !s.locks.locks[S] {
				s.locks.mu.Unlock()
				time.Sleep(3 * time.Second)
				continue
			}
			s.locks.locks[S] = false
			s.locks.mu.Unlock()
			fmt.Printf("Successfully released lock for client %d on server %d\n", S, s.serverID)
			break
		}
	}
	if R != -1 {
		for j := 0; j < 1000; j++ {
			fmt.Printf("Attempting to release lock for client %d on server %d\n", R, s.serverID)
			s.locks.mu.Lock()
			if !s.locks.locks[R] {
				s.locks.mu.Unlock()
				time.Sleep(3 * time.Second)
				continue
			}
			s.locks.locks[R] = false
			s.locks.mu.Unlock()
			fmt.Printf("Successfully released lock for client %d on server %d\n", R, s.serverID)
			break
		}
	}
}

func (s *twoPCServer) shardForID(userID int) int {
	if userID < 0 || userID > 3000 {
		fmt.Println("Error retrieving shard for userID: userID out of range")
		return -1
	}
	return (userID - 1) / (3000 / clusters)
}

func (s *twoPCServer) retrieveBalance(client string) int {
	// Verify balance
	var result bson.M
	sender, _ := strconv.Atoi(client)
	sender = sender - 1 - (1000 * s.clusterID)
	if err := s.DB.FindOne(context.Background(), bson.M{"_id": s.serverID}).Decode(&result); err != nil {
		fmt.Print(err)
	}
	bal := result["balances"].(bson.A)
	balance, _ := strconv.Atoi(fmt.Sprintf("%d", bal[sender]))
	return balance
}

func (s *twoPCServer) executeIntraTransaction(seq int) {
	entry := s.log[seq]
	fmt.Printf("Attempting execution of seq %d at server %d\n", seq, s.serverID)
	if entry == nil || entry.status != "C" {
		fmt.Printf("Tx %d not ready to execute on server %d\n", seq, s.serverID)
		return
	}

	req := entry.req
	// success := false
	fmt.Printf("Executing transaction %v on server %d\n", req.Tx, s.serverID)
	// Update local balance with tx from other servers
	amount, _ := strconv.Atoi(req.Tx.Amt)
	senderBal := s.retrieveBalance(req.Tx.S) - amount
	receiverBal := s.retrieveBalance(req.Tx.R) + amount
	sender, _ := strconv.Atoi(req.Tx.S)
	receiver, _ := strconv.Atoi(req.Tx.R)
	s.releaseLocks(sender, receiver)
	sender = sender - 1 - (1000 * s.clusterID)
	receiver = receiver - 1 - (1000 * s.clusterID)
	// Update the document with the new value
	update := bson.M{
		"$set": bson.M{
			fmt.Sprintf("balances.%d", sender):   senderBal,
			fmt.Sprintf("balances.%d", receiver): receiverBal,
		},
	}

	_, err := s.DB.UpdateOne(context.Background(), bson.M{"_id": s.serverID}, update)
	if err != nil {
		log.Fatalf("Failed to update document: %v", err)
	}
	fmt.Printf("server %d committing tx: \n%v\n", s.serverID, req.Tx)
	// Commit MB to datastore
	s.datastore = append(s.datastore, &twoPC.DSEntry{Seq: int64(seq), Tx: req.Tx})

	entry.status = "E"
	s.executedMsgs[seq] = true
	s.lastExecuted = seq
	// Attach success value to reply
	// TODO: send reply to client, store this latest reply object in s.lastReply map for this client(req.Tx.ClientID)
	s.clientConn.ReplyToClient(context.Background(), &twoPC.Reply{Timestamp: req.Timestamp, Result: "Success"})
	nextSeq := seq + 1
	if entry, exists := s.log[nextSeq]; exists && entry.status == "C" {
		fmt.Printf("Server %d has proceeding tx's unexecuted, executing now\n", s.serverID)
		s.executeIntraTransaction(nextSeq)
	} else {
		fmt.Printf("Server %d finished processing all tx's.\n", s.serverID)
	}
}

func (s *twoPCServer) executeTransaction(seq int, who string) {
	entry := s.log[seq]
	fmt.Printf("Attempting execution of seq %d at server %d\n", seq, s.serverID)
	if entry == nil || entry.status != "C" {
		fmt.Printf("Tx %d not ready to execute on server %d\n", seq, s.serverID)
		return
	}

	req := entry.req
	fmt.Printf("Executing transaction %v on server %d\n", req.Tx, s.serverID)
	// Update local balance with tx from other servers
	amount, _ := strconv.Atoi(req.Tx.Amt)
	sender, _ := strconv.Atoi(req.Tx.S)
	receiver, _ := strconv.Atoi(req.Tx.R)

	var update primitive.M
	if who == "S" {
		senderBal := s.retrieveBalance(req.Tx.S) - amount
		sender = sender - 1 - (1000 * s.clusterID)
		walAmt := amount
		// IF ABORT, GRAB WAL RECORDS AND UNDO EXECUTION(BY DOING OPPOSITE OP)
		if entry.twoPCState == "A" {
			fmt.Printf("Tx %d aborted on server %d, undoing from WAL\n", seq, s.serverID)
			// Verify balance
			var result bson.M
			if err := s.DB.FindOne(context.Background(), bson.M{"_id": s.serverID}).Decode(&result); err != nil {
				fmt.Print(err)
			}
			walAmt := result["walAmt"].(bson.A)
			walChange, _ := strconv.Atoi(fmt.Sprintf("%d", walAmt[sender]))
			senderBal = s.retrieveBalance(req.Tx.S) + walChange
		}
		// ONLY RELEASE LOCKS ONCE THIS IS IN COMMIT/ABORT PHASE
		if entry.twoPCState == "C" {
			sender, _ := strconv.Atoi(req.Tx.S)
			s.releaseLocks(sender, -1)
			// If committing, delete wal records(replace with empty/0)
			senderBal = s.retrieveBalance(req.Tx.S)
			walAmt = 0
		}
		// Update the document with the new value
		update = bson.M{
			"$set": bson.M{
				fmt.Sprintf("balances.%d", sender): senderBal,
				fmt.Sprintf("walAmt.%d", seq):      walAmt,
			},
		}
	} else if who == "R" {
		receiverBal := s.retrieveBalance(req.Tx.R) + amount
		receiver = receiver - 1 - (1000 * s.clusterID)
		walAmt := amount
		// IF ABORT, GRAB WAL RECORDS AND UNDO EXECUTION(BY DOING OPPOSITE OP)
		if entry.twoPCState == "A" {
			fmt.Printf("Tx %d aborted on server %d, undoing from WAL\n", seq, s.serverID)
			// Verify balance
			var result bson.M
			if err := s.DB.FindOne(context.Background(), bson.M{"_id": s.serverID}).Decode(&result); err != nil {
				fmt.Print(err)
			}
			walAmt := result["walAmt"].(bson.A)
			walChange, _ := strconv.Atoi(fmt.Sprintf("%d", walAmt[receiver]))
			receiverBal = s.retrieveBalance(req.Tx.R) - walChange
		}
		// ONLY RELEASE LOCKS ONCE THIS IS IN COMMIT/ABORT PHASE
		if entry.twoPCState == "C" {
			receiver, _ := strconv.Atoi(req.Tx.R)
			s.releaseLocks(-1, receiver)
			// If committing, delete wal records(replace with empty/0)
			receiverBal = s.retrieveBalance(req.Tx.R)
			walAmt = 0
		}
		// Update the document with the new value
		update = bson.M{
			"$set": bson.M{
				fmt.Sprintf("balances.%d", receiver): receiverBal,
				fmt.Sprintf("walAmt.%d", seq):        walAmt,
			},
		} 
	}
	// TODO: check the else if its correct for non "C" states, and check the
	// client reply conditions/what to send back(based on abort/commit). then start on sending prepare with 2f+1 commitmsgs to
	// leader of participant cluster and start participant prepare

	_, err := s.DB.UpdateOne(context.Background(), bson.M{"_id": s.serverID}, update)
	if err != nil {
		log.Fatalf("Failed to update document: %v", err)
	}

	// Commit MB to datastore; EVERYTHING EXCEPT ABORT TXs
	if entry.twoPCState != "A" {
		s.datastore = append(s.datastore, &twoPC.DSEntry{Seq: int64(seq), Tx: req.Tx, Status: entry.twoPCState})
	}

	entry.status = "E"
	s.executedMsgs[seq] = true
	s.lastExecuted = seq
	// TODO: CHECK IF THIS IS CORECT, AND ALSO SETUP WHAT PARTICIPANT CLUSETRS(Who="R") DO HERE
	if entry.twoPCState == "A" && who == "S" {
		s.clientConn.ReplyToClient(context.Background(), &twoPC.Reply{Timestamp: req.Timestamp})
	}
	nextSeq := seq + 1
	if entry, exists := s.log[nextSeq]; exists && entry.status == "C" {
		fmt.Printf("Server %d has proceeding tx's unexecuted, executing now\n", s.serverID)
		s.executeTransaction(nextSeq, who)
	} else {
		fmt.Printf("Server %d finished processing all tx's.\n", s.serverID)
	}
}

// ---------------CLIENT STATUS REQUEST STUFF-------------------

// DONE
// Changes liveness setting based on liveservers set in input csv
func (s *twoPCServer) NotifyLiveness(_ context.Context, in *twoPC.Liveness) (*twoPC.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLive = in.Live
	fmt.Printf("Server %d is live?:%t\n", s.serverID, s.isLive)
	return &twoPC.Empty{}, nil
}

// DONE
// Changes byzantine setting based on byzantineservers set in input csv
func (s *twoPCServer) NotifyByzantine(_ context.Context, in *twoPC.Byzantine) (*twoPC.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isByzantine = in.Byzantine
	fmt.Printf("Server %d is byzantine?:%t\n", s.serverID, s.isByzantine)
	return &twoPC.Empty{}, nil
}

// DONE
// Process request for balance of given id
func (s *twoPCServer) RequestBalance(_ context.Context, in *twoPC.BalanceRequest) (*twoPC.BalanceResponse, error) {
	fmt.Printf("Received request for balance of client %s at server %d\n", in.ClientID, s.serverID)
	return &twoPC.BalanceResponse{Balance: fmt.Sprintf("%d", s.retrieveBalance(in.ClientID))}, nil
}

// DONE
// Process request for DB at server
func (s *twoPCServer) RequestDS(_ context.Context, in *twoPC.ServerRequested) (*twoPC.DSResponse, error) {
	res := []*twoPC.DSEntry{}
	res = append(res, s.datastore...)
	return &twoPC.DSResponse{DS: res}, nil
}

// DONE
// Process client transaction
func (s *twoPCServer) RequestTransaction(ctx context.Context, in *twoPC.TxRequest) (*twoPC.Empty, error) {
	fmt.Printf("Received tx at server %d: %v\n", s.serverID, in)
	s.mu.Lock()

	// If not primary, forward(or do nothing idk)
	if s.serverID != 1 && s.serverID != 5 && s.serverID != 9 {
		s.mu.Unlock()
		return &twoPC.Empty{}, nil
	} else if s.prePreparedMsgs[int(in.Timestamp)] != nil {
		if s.log[int(s.prePreparedMsgs[int(in.Timestamp)].Seq)].status != "E" {
			// If preprepared and not executed already, rebroadcast
			fmt.Printf("Leader %d has received forwarded request from client with ts %d: %v\n", s.serverID, in.Timestamp, in.Tx)
			s.mu.Unlock()
			go s.BroadcastPrePrepare(ctx, s.prePreparedMsgs[int(in.Timestamp)])
			return &twoPC.Empty{}, nil
		}
	}

	S, _ := strconv.Atoi(in.Tx.S)
	R, _ := strconv.Atoi(in.Tx.R)
	Amt, _ := strconv.Atoi(in.Tx.Amt)

	s.acquireLocks(S, R)
	if s.retrieveBalance(in.Tx.S) < Amt {
		fmt.Printf("Insufficient balance for client %s at leader %d, ignoring..\n", in.Tx.S, s.serverID)
		s.releaseLocks(S, R)
		s.mu.Unlock()
		return &twoPC.Empty{}, nil
	}

	s.seq++
	prePrepare := &twoPC.PrePrepare{View: int64(s.currPrimary), Seq: int64(s.seq), Request: in, Signature: in.Signature, Who: "", TwoPCState: ""}
	s.prePreparedMsgs[int(in.Timestamp)] = prePrepare

	s.log[s.seq] = &LogEntry{
		req:           in,
		prePrepareMsg: prePrepare,
		prepareMsgs:   make(map[int]*twoPC.Prepare),
		commitMsgs:    make(map[int]*twoPC.Commit),
		status:        "PP",
		twoPCState:    "",
	}

	s.mu.Unlock()
	go s.BroadcastPrePrepare(ctx, prePrepare)
	return &twoPC.Empty{}, nil
}

// ---------------INTRA SHARD LEADER STUFF-------------------

// Leader PrePrepare
func (s *twoPCServer) BroadcastPrePrepare(ctx context.Context, msg *twoPC.PrePrepare) {
	repliesReceived := 0
	for _, server := range s.serverConns {
		res, err := server.ProcessPrePrepare(context.Background(), msg)
		if err != nil {
			fmt.Printf("errored while broadcasting pre-prepare: %v", err)
			continue
		}
		if res != nil {
			// Increment num replies and store prepare msg in log entry for tx
			repliesReceived++
			s.log[int(msg.Seq)].prepareMsgs[int(res.ReplicaID)] = res
			fmt.Printf("Leader %d received prepare reply from replica %d for tx %d\n", s.serverID, res.ReplicaID, msg.Seq)
			// Check if replies match, get 2f sigs and collect and append them to new prepare
		}
	}

	if repliesReceived >= 2 {
		fmt.Printf("Leader %d received enough prepares, broadcasting leader prepare\n", s.serverID)
		s.BroadcastPrepare(ctx, &twoPC.Prepare{View: msg.View, Seq: msg.Seq, ReplicaID: int64(s.serverID)})
	} else {
		fmt.Printf("Leader %d did not receive majority, exiting...\n", s.serverID)
	}
}

// Leader Prepare
func (s *twoPCServer) BroadcastPrepare(ctx context.Context, msg *twoPC.Prepare) {
	if s.isByzantine {
		fmt.Println("Leader is Byzantine, no prepares will be sent")
		return
	}
	repliesReceived := 0
	for _, server := range s.serverConns {
		fmt.Println("Broadcasting prepare to replica")
		res, err := server.ProcessPrepare(context.Background(), msg)
		if err != nil {
			fmt.Printf("errored while broadcasting prepare: %v", err)
			continue
		}
		if res != nil {
			repliesReceived++
			// Store preparemsg for currTX on this server's log; change its status to prepared
			s.log[int(msg.Seq)].commitMsgs[int(res.ReplicaID)] = res
			// Check if replies match, get 2f sigs and collect and append them to new prepare
		}
	}
	if repliesReceived >= 2 {
		s.log[int(msg.Seq)].status = "P"
		fmt.Println("Leader received enough commits, broadcasting leader commit")
		s.BroadcastCommit(ctx, &twoPC.Commit{View: msg.View, Seq: msg.Seq, ReplicaID: int64(s.serverID)})
	}
}

// Leader Commit
func (s *twoPCServer) BroadcastCommit(ctx context.Context, msg *twoPC.Commit) {
	for _, server := range s.serverConns {
		fmt.Println("Broadcasting commit to replica")
		res, err := server.ProcessCommit(context.Background(), msg)
		if err != nil {
			fmt.Printf("errored while broadcasting commit: %v", err)
			continue
		}
		if res != nil {
			continue
		}
	}
	s.log[int(msg.Seq)].status = "C"
	for i := 1; i < int(msg.Seq); i++ {
		if !s.executedMsgs[i] {
			fmt.Printf("Leader hasn't executed all reqs with seq less than %d", msg.Seq)
			fmt.Printf("Leader %d executed so far: %v\n", s.serverID, s.executedMsgs)
			return
		}
	}
	if s.lastExecuted == int(msg.Seq)-1 {
		// Execute func? then send reply to client
		fmt.Printf("Leader executing tx %d, last executed %d\n", msg.Seq, s.lastExecuted)
		if msg.Who != "S" && msg.Who != "R" {
			s.executeIntraTransaction(int(msg.Seq))
		} else {
			s.executeTransaction(int(msg.Seq), "")
		}
	}
}

// ---------------INTRA SHARD FOLLOWER STUFF-------------------

// Replica send Prepare after leader PrePrepare
func (s *twoPCServer) ProcessPrePrepare(ctx context.Context, in *twoPC.PrePrepare) (*twoPC.Prepare, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isLive {
		return nil, fmt.Errorf("disconnected replica, can't accept pre-prepare")
	}

	fmt.Printf("Replica %d processing pre-prepare %v\n", s.serverID, in)

	if _, exists := s.log[int(in.Seq)]; !exists {
		s.log[int(in.Seq)] = &LogEntry{
			req:           in.Request,
			prePrepareMsg: in,
			prepareMsgs:   make(map[int]*twoPC.Prepare),
			commitMsgs:    make(map[int]*twoPC.Commit),
			status:        "X",
			twoPCState:    in.TwoPCState,
		}
	}

	if s.log[int(in.Seq)].status != "X" && s.log[int(in.Seq)].status != "E" {
		fmt.Printf("Replica %d has already processed pre-prepare %v, proceeding without reacquiring locks\n", s.serverID, in)
		// Setup Prepare reply to leader
		prepare := &twoPC.Prepare{View: in.View, Seq: in.Seq, ReplicaID: int64(s.serverID)}
		return prepare, nil
	} else {
		S, _ := strconv.Atoi(in.Request.Tx.S)
		R, _ := strconv.Atoi(in.Request.Tx.R)
		if !s.isByzantine {
			if in.Who == "S" && in.TwoPCState == "P" {
				s.acquireLocks(S, -1)
			} else if in.Who == "R" && in.TwoPCState == "P" {
				s.acquireLocks(-1, R)
			} else if in.Who != "S" && in.Who != "R" {
				s.acquireLocks(S, R)
			}
		}
		s.log[int(in.Seq)].status = "PP"
	}

	// Setup Prepare reply to leader
	prepare := &twoPC.Prepare{View: in.View, Seq: in.Seq, ReplicaID: int64(s.serverID)}
	return prepare, nil
}

// Replica send Commit after leader Prepare
func (s *twoPCServer) ProcessPrepare(ctx context.Context, in *twoPC.Prepare) (*twoPC.Commit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isLive {
		return nil, fmt.Errorf("disconnected replica, can't accept prepare")
	}
	if s.isByzantine {
		return nil, fmt.Errorf("replica %d is byzantine, no commit being sent back", s.serverID)
	}
	entry := s.log[int(in.Seq)]
	if entry == nil {
		return nil, fmt.Errorf("no matching log entry for prepare")
	}
	// Store preparemsg for currTX on this server's log; change its status to prepared
	entry.prepareMsgs[int(in.ReplicaID)] = in
	entry.status = "P"

	fmt.Printf("Replica %d processing prepare %v\n", s.serverID, in)

	// Setup Prepare reply to leader
	commit := &twoPC.Commit{View: in.View, Seq: in.Seq, ReplicaID: int64(s.serverID)}

	return commit, nil
}

// Replica execute after leader Commit
func (s *twoPCServer) ProcessCommit(ctx context.Context, in *twoPC.Commit) (*twoPC.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isLive {
		return nil, fmt.Errorf("disconnected replica, can't accept commit")
	}
	// If byzantine, don't run/execute
	if s.isByzantine {
		return nil, fmt.Errorf("byzantine replica, can't accept commit")
	}

	entry := s.log[int(in.Seq)]
	if entry == nil {
		return &twoPC.Empty{}, fmt.Errorf("no matching log entry for commit")
	}
	if entry.status != "P" {
		return &twoPC.Empty{}, fmt.Errorf("replica probably byzantine/disconnected, tx still hasn't been prepared")
	}
	entry.commitMsgs[int(in.ReplicaID)] = in
	entry.status = "C"
	fmt.Printf("Replica %d processing commit %v\n", s.serverID, in)
	for i := 1; i < int(in.Seq); i++ {
		if !s.executedMsgs[i] {
			fmt.Printf("Replica %d hasn't executed previous seq before %d, not replying with commit\n", s.serverID, in.Seq)
			return &twoPC.Empty{}, nil
		}
	}

	if s.lastExecuted == int(in.Seq)-1 {
		// Execute func? then send reply to client
		if in.Who != "S" && in.Who != "R" {
			s.executeIntraTransaction(int(in.Seq))
		} else {
			s.executeTransaction(int(in.Seq), in.Who)
			// If participants done executing in commit phase, also send ACKS to coordinator leader
			if in.Who == "R" {
				S, _ := strconv.Atoi(s.log[int(in.Seq)].req.Tx.S)
				shard := s.shardForID(S)
				leader := (shard * clusterSize) + 1
				fmt.Printf("Participant follower %d sending committed for tx %d to coordinator cluster w/ leader %d\n", s.serverID, in.Seq, leader)

				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 50050+leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				serverConn := twoPC.NewTwoPCClient(conn)

				ack := &twoPC.Ack{
					ServerID:    int64(s.serverID),
					ReceiverSeq: int64(s.log[int(in.Seq)].senderSeq),
					SenderSeq:   in.Seq,
				}

				// Send ack back to coordinator
				serverConn.CoordinatorAck(context.Background(), ack)
			}

		}
	}
	return &twoPC.Empty{}, nil
}

// ---------------CROSS SHARD CONSENSUS STUFF-------------------

func (s *twoPCServer) LeaderBroadcastPrePrepare(ctx context.Context, msg *twoPC.PrePrepare) {
	s.mu.Lock()
	repliesReceived := 0
	for _, server := range s.serverConns {
		res, err := server.ProcessPrePrepare(context.Background(), msg)
		if err != nil {
			fmt.Printf("errored while broadcasting pre-prepare: %v", err)
			continue
		}
		if res != nil {
			// Increment num replies and store prepare msg in log entry for tx
			repliesReceived++
			s.log[int(msg.Seq)].prepareMsgs[int(res.ReplicaID)] = res
			fmt.Printf("Leader %d received prepare reply from replica %d for tx %d\n", s.serverID, res.ReplicaID, msg.Seq)
		}
	}

	if repliesReceived >= 2 {
		s.mu.Unlock()
		fmt.Printf("Leader %d received enough prepares, broadcasting leader prepare\n", s.serverID)
		s.LeaderBroadcastPrepare(ctx, &twoPC.Prepare{View: msg.View, Seq: msg.Seq, ReplicaID: int64(s.serverID), TwoPCState: msg.TwoPCState, Who: msg.Who})
	} else {
		s.mu.Unlock()
		fmt.Printf("Leader %d did not receive majority, exiting...\n", s.serverID)
	}
}

func (s *twoPCServer) LeaderBroadcastPrepare(ctx context.Context, msg *twoPC.Prepare) {
	s.mu.Lock()
	if s.isByzantine {
		s.mu.Unlock()
		fmt.Println("Leader is Byzantine, no prepares will be sent")
		return
	}
	repliesReceived := 0
	for _, server := range s.serverConns {
		fmt.Println("Broadcasting prepare to replica")
		res, err := server.ProcessPrepare(context.Background(), msg)
		if err != nil {
			fmt.Printf("errored while broadcasting prepare: %v", err)
			continue
		}
		if res != nil {
			repliesReceived++
			// Store preparemsg for currTX on this server's log; change its status to prepared
			s.log[int(msg.Seq)].commitMsgs[int(res.ReplicaID)] = res
		}
	}
	if repliesReceived >= 2 {
		s.log[int(msg.Seq)].status = "P"
		fmt.Println("Leader received enough commits, broadcasting leader commit")
		s.mu.Unlock()
		s.LeaderBroadcastCommit(ctx, &twoPC.Commit{View: msg.View, Seq: msg.Seq, ReplicaID: int64(s.serverID), TwoPCState: msg.TwoPCState, Who: msg.Who})
	} else {
		s.mu.Unlock()
	}
}

func (s *twoPCServer) LeaderBroadcastCommit(ctx context.Context, msg *twoPC.Commit) {
	s.mu.Lock()
	var commitMsgs []*twoPC.Commit
	var repliesReceived int
	for _, server := range s.serverConns {
		fmt.Println("Broadcasting commit to replica")
		res, err := server.ProcessCommit(context.Background(), msg)
		if err != nil {
			fmt.Printf("errored while broadcasting commit: %v", err)
			continue
		}
		if res != nil {
			// TODO: gather 2f(plus own, so 2f+1) correct commitmsgs to forward later
			// newCommit := &twoPC.Commit{View: msg.View, Seq: msg.Seq, ReplicaID: int64(s.serverID), TwoPCState: msg.TwoPCState}
			repliesReceived++
			commitMsgs = append(commitMsgs, msg)
			continue
		}
	}
	s.log[int(msg.Seq)].status = "C"
	for i := 1; i < int(msg.Seq); i++ {
		if !s.executedMsgs[i] {
			fmt.Printf("Leader hasn't executed all reqs with seq less than %d", msg.Seq)
			fmt.Printf("Leader %d executed so far: %v\n", s.serverID, s.executedMsgs)
			s.mu.Unlock()
			return
		}
	}
	if s.lastExecuted == int(msg.Seq)-1 {
		// Execute func? then send reply to client
		fmt.Printf("Leader executing tx %d, last executed %d\n", msg.Seq, s.lastExecuted)
		s.mu.Unlock()
		s.executeTransaction(int(msg.Seq), msg.Who)
	}
	// 2f+1 commit, send prepare to participant
	if repliesReceived >= 2 {
		// Coordinator comms to participant
		if msg.Who == "S" {
			if msg.TwoPCState == "C" {
				s.mu.Lock()
				R, _ := strconv.Atoi(s.log[int(msg.Seq)].req.Tx.R)
				shard := s.shardForID(R)
				leader := (shard * clusterSize) + 1
				fmt.Printf("Leader %d sending commit for tx %d to participant cluster w/ leader %d\n", s.serverID, msg.Seq, leader)
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 50050+leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					s.mu.Unlock()
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				serverConn := twoPC.NewTwoPCClient(conn)

				request := &twoPC.TwoPCCommit{
					SenderID:  int64(s.serverID),
					Req:       s.log[int(msg.Seq)].req,
					Outcome:   "C",
					SenderSeq: msg.Seq,
				}
				// TODO: start timer here, react differently based on whether its a P or C state( Prepare means abort stuff, C means resend to
				// participant)
				// COMMIT PHASE TIMERS SHUD RESEND TO ANY THAT ARE SLOW INDEFINITELY UNTIL IT RECEIVES 2 ACKS BACK

				s.mu.Unlock()
				res, err := serverConn.ParticipantCommit(context.Background(), request)
				if err != nil {
					fmt.Printf("errored while sending prepare to participant: %v", err)
				}
				if res != nil {
					fmt.Printf("Coordinator %d received ack of commit decision from participant\n", s.serverID)
				}
				s.mu.Lock()
				s.timers[int(msg.Seq)] = time.NewTimer(50 * time.Second)
				s.mu.Unlock()
				go func() {
					<-s.timers[int(msg.Seq)].C
					if s.commitsReceived[int(msg.Seq)] < 2 {
						fmt.Printf("Commit phase timer at coordinator %d ran out for tx %d, resending commi to slow servers\n", s.serverID, msg.Seq)
						// go c.sendTransaction(tx, ts)
					}
				}()
			} else {
				s.mu.Lock()
				R, _ := strconv.Atoi(s.log[int(msg.Seq)].req.Tx.R)
				shard := s.shardForID(R)
				leader := (shard * clusterSize) + 1
				fmt.Printf("Leader %d sending prepare for tx %d to participant cluster w/ leader %d\n", s.serverID, msg.Seq, leader)
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 50050+leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					s.mu.Unlock()
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				serverConn := twoPC.NewTwoPCClient(conn)

				request := &twoPC.TwoPCPrepare{
					SenderID:   int64(s.serverID),
					Req:        s.log[int(msg.Seq)].req,
					CommitMsgs: commitMsgs,
					SenderSeq:  msg.Seq,
				}
				// TODO: start timer here, react differently based on whether its a P or C state( Prepare means abort stuff, C means resend to
				// participant)
				// PREPARE PHASE TIMER TIMES OUT, RUN CONSENSUS ON ABORTED TX

				s.mu.Unlock()
				_, err = serverConn.ParticipantPrepare(context.Background(), request)
				if err != nil {
					fmt.Printf("errored while sending prepare to participant: %v", err)
				}
				s.mu.Lock()
				s.timers[int(msg.Seq)] = time.NewTimer(50 * time.Second)
				s.mu.Unlock()

				go func() {
					<-s.timers[int(msg.Seq)].C
					// TODO: ABORT HERE IF PARTICIPANT DOESNT SEND PREPARED BACK IN TIME
				}()
			}
		} else {
			// Participant comms to coordinator
			if msg.TwoPCState == "C" {
				s.mu.Lock()
				S, _ := strconv.Atoi(s.log[int(msg.Seq)].req.Tx.S)
				shard := s.shardForID(S)
				leader := (shard * clusterSize) + 1
				fmt.Printf("Leader %d sending committed for tx %d to coordinator cluster w/ leader %d\n", s.serverID, msg.Seq, leader)

				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 50050+leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				serverConn := twoPC.NewTwoPCClient(conn)

				ack := &twoPC.Ack{
					ServerID:    int64(s.serverID),
					ReceiverSeq: int64(s.log[int(msg.Seq)].senderSeq),
					SenderSeq:   msg.Seq,
				}

				// Send ack back to coordinator
				s.mu.Unlock()
				serverConn.CoordinatorAck(context.Background(), ack)

			} else if msg.TwoPCState == "P" {
				s.mu.Lock()
				S, _ := strconv.Atoi(s.log[int(msg.Seq)].req.Tx.S)
				shard := s.shardForID(S)
				leader := (shard * clusterSize) + 1
				fmt.Printf("Leader %d sending prepared for tx %d to coordinator cluster w/ leader %d\n", s.serverID, msg.Seq, leader)
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 50050+leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					s.mu.Unlock()
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				serverConn := twoPC.NewTwoPCClient(conn)

				request := &twoPC.TwoPCPrepare{
					SenderID:   int64(s.serverID),
					Req:        s.log[int(msg.Seq)].req,
					CommitMsgs: commitMsgs,
					SenderSeq:  int64(s.log[int(msg.Seq)].senderSeq),
				}

				s.mu.Unlock()
				_, err = serverConn.CoordinatorCommit(context.Background(), request)
				if err != nil {
					fmt.Printf("errored while sending prepare to participant: %v", err)
				}
			}

		}
	} else {
		s.mu.Unlock()
	}
}

// ---------------CROSS SHARD COORDINATOR STUFF-------------------

func (s *twoPCServer) CoordinatorPrepare(ctx context.Context, in *twoPC.TxRequest) (*twoPC.Empty, error) {
	s.mu.Lock()
	// If not primary, forward(or do nothing idk)
	if s.serverID != 1 && s.serverID != 5 && s.serverID != 9 {
		s.mu.Unlock()
		return &twoPC.Empty{}, nil
	} else if s.prePreparedMsgs[int(in.Timestamp)] != nil {
		if s.log[int(s.prePreparedMsgs[int(in.Timestamp)].Seq)] != nil {
			// If preprepared and not executed already, rebroadcast
			fmt.Printf("Leader %d has received forwarded request from client with ts %d: %v\n", s.serverID, in.Timestamp, in.Tx)
			s.mu.Unlock()
			go s.LeaderBroadcastPrePrepare(ctx, s.prePreparedMsgs[int(in.Timestamp)])
			return &twoPC.Empty{}, nil
		}
	}

	S, _ := strconv.Atoi(in.Tx.S)
	Amt, _ := strconv.Atoi(in.Tx.Amt)

	// Unavailable lock: wait
	s.mu.Unlock()
	s.acquireLocks(S, -1)
	if s.retrieveBalance(in.Tx.S) < Amt {
		// Insufficient balance: ignore/skip
		fmt.Printf("Insufficient balance for client %s at leader %d, ignoring..\n", in.Tx.S, s.serverID)
		s.releaseLocks(S, -1)
		return &twoPC.Empty{}, nil
	}

	s.mu.Lock()

	s.seq++
	prePrepare := &twoPC.PrePrepare{View: int64(s.currPrimary), Seq: int64(s.seq), Request: in, Signature: in.Signature, TwoPCState: "P", Who: "S"}
	s.prePreparedMsgs[int(in.Timestamp)] = prePrepare

	s.log[s.seq] = &LogEntry{
		req:           in,
		prePrepareMsg: prePrepare,
		prepareMsgs:   make(map[int]*twoPC.Prepare),
		commitMsgs:    make(map[int]*twoPC.Commit),
		status:        "PP",
		twoPCState:    "P",
	}

	s.mu.Unlock()
	go s.LeaderBroadcastPrePrepare(ctx, prePrepare)
	return &twoPC.Empty{}, nil
}

func (s *twoPCServer) CoordinatorCommit(ctx context.Context, in *twoPC.TwoPCPrepare) (*twoPC.Empty, error) {
	s.mu.Lock()
	fmt.Printf("Leader %d of coordinator cluster beginning commit phase\n", s.serverID)

	stopped := s.timers[int(in.SenderSeq)].Stop()
	if stopped {
		fmt.Printf("Stopped timer at coordinator %d for tx %d as got prepare back\n", s.serverID, in.SenderSeq)
	}
	s.seq++
	prePrepare := &twoPC.PrePrepare{View: int64(s.currPrimary), Seq: int64(s.seq), Request: in.Req, Signature: in.Req.Signature, TwoPCState: "C", Who: "S"}
	s.prePreparedMsgs[int(in.Req.Timestamp)] = prePrepare

	s.log[s.seq] = &LogEntry{
		req:           in.Req,
		prePrepareMsg: prePrepare,
		prepareMsgs:   make(map[int]*twoPC.Prepare),
		commitMsgs:    make(map[int]*twoPC.Commit),
		status:        "PP",
		twoPCState:    "C",
	}

	s.mu.Unlock()
	go s.LeaderBroadcastPrePrepare(ctx, prePrepare)
	return &twoPC.Empty{}, nil
}

func (s *twoPCServer) CoordinatorAck(Ctx context.Context, in *twoPC.Ack) (*twoPC.Empty, error) {
	s.commitsReceived[int(in.ReceiverSeq)]++
	if s.commitsReceived[int(in.ReceiverSeq)] >= 2 {
		// stopped := s.timers[int(in.ReceiverSeq)].Stop()
		// if stopped {
		// 	fmt.Printf("Stopped timer at coordinator %d for tx %d as got enough ACKS\n", s.serverID, in.ReceiverSeq)
		// }
	}
	return &twoPC.Empty{}, nil
}

// ---------------CROSS SHARD PARTICIPANT STUFF-------------------
func (s *twoPCServer) ParticipantPrepare(ctx context.Context, msg *twoPC.TwoPCPrepare) (*twoPC.Empty, error) {
	s.mu.Lock()
	fmt.Printf("Participant leader %d beginning prepare phase\n", s.serverID)

	R, _ := strconv.Atoi(msg.Req.Tx.R)

	// Unavailable lock: wait
	s.mu.Unlock()
	s.acquireLocks(-1, R)

	s.mu.Lock()

	s.seq++
	prePrepare := &twoPC.PrePrepare{View: int64(s.currPrimary), Seq: int64(s.seq), Request: msg.Req, TwoPCState: "P", Who: "R"}
	s.prePreparedMsgs[int(msg.Req.Timestamp)] = prePrepare

	s.log[s.seq] = &LogEntry{
		req:           msg.Req,
		prePrepareMsg: prePrepare,
		prepareMsgs:   make(map[int]*twoPC.Prepare),
		commitMsgs:    make(map[int]*twoPC.Commit),
		status:        "PP",
		twoPCState:    "P",
		senderSeq:     int(msg.SenderSeq),
	}

	s.mu.Unlock()
	go s.LeaderBroadcastPrePrepare(ctx, prePrepare)
	return &twoPC.Empty{}, nil
}

func (s *twoPCServer) ParticipantCommit(ctx context.Context, in *twoPC.TwoPCCommit) (*twoPC.Empty, error) {
	s.mu.Lock()
	fmt.Printf("Leader %d of participant cluster beginning commit phase\n", s.serverID)

	s.seq++
	prePrepare := &twoPC.PrePrepare{View: int64(s.currPrimary), Seq: int64(s.seq), Request: in.Req, Signature: in.Req.Signature, TwoPCState: "C", Who: "R"}
	s.prePreparedMsgs[int(in.Req.Timestamp)] = prePrepare

	s.log[s.seq] = &LogEntry{
		req:           in.Req,
		prePrepareMsg: prePrepare,
		prepareMsgs:   make(map[int]*twoPC.Prepare),
		commitMsgs:    make(map[int]*twoPC.Commit),
		status:        "PP",
		twoPCState:    "C",
		senderSeq:     int(in.SenderSeq),
	}

	s.mu.Unlock()
	go s.LeaderBroadcastPrePrepare(ctx, prePrepare)
	return &twoPC.Empty{}, nil
}

// ---------------CROSS SHARD FOLLOWER STUFF-------------------

// ---------------SERVER STATE STUFF-------------------

// Set up a connection to the other servers and the client
func (s *twoPCServer) connectServers() {
	for i := s.clusterID*clusterSize + 1; i <= (s.clusterID+1)*clusterSize; i++ {
		if i == s.serverID {
			continue
		}
		conn, err := grpc.NewClient(fmt.Sprintf("localhost: %d", 50050+i), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		c := twoPC.NewTwoPCClient(conn)
		s.serverConns = append(s.serverConns, c)
	}
	conn, err := grpc.NewClient("localhost: 40040", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	s.clientConn = twoPC.NewTwoPCClient(conn)
}

// Set up conn to Mongo
func (s *twoPCServer) connectMongo() {
	// Use the SetServerAPIOptions() method to set the version of the Stable API on the client
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI("mongodb://localhost:27017").SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	// defer func() {
	// 	if err = client.Disconnect(context.TODO()); err != nil {
	// 		panic(err)
	// 	}
	// }()
	// Send a ping to confirm a successful connection
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")

	s.DB = client.Database("ProgramData").Collection("ServerData")

	// Setup initial server document/data entry in collection

	coll := client.Database("ProgramData").Collection("ServerData")
	// Delete old data from previous runs of entire program
	coll.DeleteOne(context.TODO(), bson.D{{"_id", s.serverID}})
	clients := make([]int, 1000)
	for i := 0; i < 1000; i++ {
		clients[i] = 10
	}
	doc := bson.D{{"_id", s.serverID}, {"balances", clients}, {"walAmt", make([]int, 100)}}

	coll.InsertOne(context.Background(), doc)
}

// Start servers and connect them to each other
func startServer(serverID int, group *sync.WaitGroup) {
	defer group.Done()
	serverPort := fmt.Sprintf("%d", 50050+serverID)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", serverPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Initialize servers and connect them to each other
	server := grpc.NewServer()
	twoPCServ := &twoPCServer{serverID: serverID,
		clusterID:   (serverID - 1) / clusterSize,
		log:         make(map[int]*LogEntry),
		datastore:   make([]*twoPC.DSEntry, 0),
		locks:       &LockMap{locks: make(map[int]bool)},
		preparedTxs: make(map[int]bool), isLive: false,
		isByzantine:     false,
		seq:             0,
		lastExecuted:    0,
		prePreparedMsgs: make(map[int]*twoPC.PrePrepare),
		preparedMsgs:    make(map[int]bool),
		committedMsgs:   make(map[int]bool),
		executedMsgs:    make(map[int]bool),
		timers:          make(map[int]*time.Timer),
		commitsReceived: make(map[int]int),
	}
	twoPCServ.connectServers()
	twoPCServ.connectMongo()
	twoPC.RegisterTwoPCServer(server, twoPCServ)

	log.Printf("server %d started on port %s, cluster %d\n", serverID, serverPort, twoPCServ.clusterID)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {

	var group sync.WaitGroup

	for i := 1; i <= clusters*clusterSize; i++ {
		group.Add(1)
		go startServer(i, &group)
	}
	group.Wait()
}
