package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"twoPC/twoPC"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// IMPORTANT: Change this to change input csv
var inputFile string = "Lab4_Testset_1.csv"

// DONE
type Transaction struct {
	S   int
	R   int
	Amt int
}

type TransactionResult struct {
	txId   int
	status string
}

// DONE
type TransactionSet struct {
	Num              int
	Transactions     []Transaction
	LiveServers      []int
	ByzantineServers []int
	ContactServers   []int
}

// -----------------------------CLIENT + SHARDING CODE-------------------------------------
// DONE
type Client struct {
	twoPC.UnimplementedTwoPCServer
	numClusters int
	clusterSize int
	clusters    [][]int
	txSet       TransactionSet

	timestamp  int
	privateKey *ecdsa.PrivateKey

	serverConns map[int]twoPC.TwoPCClient

	timers          map[int]*time.Timer
	repliesReceived map[int]int

	mu sync.Mutex
}

// DONE
func (c *Client) shardForID(userID int) int {
	if userID < 0 || userID > 3000 {
		fmt.Println("Error retrieving shard for userID: userID out of range")
		return -1
	}
	return (userID - 1) / (3000 / c.numClusters)
}

// DONE
func (c *Client) isIntraShard(S, R int) bool {
	senderShard := c.shardForID(S)
	receiverShard := c.shardForID(R)

	if senderShard == -1 || receiverShard == -1 {
		fmt.Printf("error determining clusters/shards")
		return false
	}

	return senderShard == receiverShard
}

// -----------------------------TEST CASE PARSING-------------------------------------
// DONE
func parseTransaction(input string) Transaction {
	transaction := strings.Split(strings.Trim(input, "()"), ",")
	Sender, err := strconv.Atoi(strings.TrimSpace(transaction[0]))
	if err != nil {
		log.Fatal("Error parsing transaction sender: ", err)
	}
	Receiver, err := strconv.Atoi(strings.TrimSpace(transaction[1]))
	if err != nil {
		log.Fatal("Error parsing transaction receiver: ", err)
	}
	Amount, err := strconv.Atoi(strings.TrimSpace(transaction[2]))
	if err != nil {
		log.Fatal("Error parsing transaction amount: ", err)
	}
	return Transaction{S: Sender, R: Receiver, Amt: Amount}
}

// DONE
func parseLiveServers(input string) []int {
	servers := strings.Split(strings.Trim(input, "[]"), ",")
	var liveServers []int

	for _, server := range servers {
		ID, err := strconv.Atoi(strings.TrimSpace(server)[1:])
		if err != nil {
			log.Fatal("Error parsing live servers: ", err)
		}
		liveServers = append(liveServers, ID)
	}

	return liveServers
}

// DONE
func parseByzantineServers(input string) []int {
	servers := strings.Split(strings.Trim(input, "[]"), ",")
	var byzantineServers []int

	for _, server := range servers {
		ID, err := strconv.Atoi(strings.TrimSpace(server)[1:])
		if err != nil {
			log.Fatal("Error parsing byzantine servers: ", err)
		}
		byzantineServers = append(byzantineServers, ID)
	}

	return byzantineServers
}

// DONE
func parseContactServers(input string) []int {
	servers := strings.Split(strings.Trim(input, "[]"), ",")
	var contactServers []int

	for _, server := range servers {
		ID, err := strconv.Atoi(strings.TrimSpace(server)[1:])
		if err != nil {
			log.Fatal("Error parsing contact servers: ", err)
		}
		contactServers = append(contactServers, ID)
	}

	return contactServers
}

// DONE
func parse(fileName string) []TransactionSet {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Can't open given CSV file from path: ", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Couldn't read CSV: ", err)
	}

	var sets []TransactionSet
	var currSet TransactionSet
	newSet := true

	for _, record := range records {
		// New test case/set
		if record[0] != "" {
			// If done reading last set, append to list of sets
			if !newSet {
				sets = append(sets, currSet)
			}
			setNum, err := strconv.Atoi(record[0])
			if err != nil {
				log.Fatal("Invalid set num: ", err)
			}
			// Create new set with parsed num
			currSet = TransactionSet{Num: setNum}

			// Parse first tx and append to list of tx for set
			currSet.Transactions = append(currSet.Transactions, parseTransaction(record[1]))

			// Parse live servers and add to currSet var
			currSet.LiveServers = parseLiveServers(record[2])
			currSet.ContactServers = parseContactServers(record[3])
			currSet.ByzantineServers = parseByzantineServers(record[4])

			newSet = false
		} else { //New tx in same set
			currSet.Transactions = append(currSet.Transactions, parseTransaction(record[1]))
		}
	}

	// Edge case; append last parsed set
	if !newSet {
		sets = append(sets, currSet)
	}

	return sets
}

// -----------------------------CLIENT TO SERVER------------------------------------
func notifyLiveness(server int, liveServers []int) {
	// Set up a connection to the server.
	conn, err := grpc.NewClient(fmt.Sprintf("localhost: %d", 50050+server), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := twoPC.NewTwoPCClient(conn)

	_, err = c.NotifyLiveness(context.Background(), &twoPC.Liveness{Live: slices.Contains(liveServers, server)})
	if err != nil {
		log.Fatalf("could not send transaction to server: %v", err)
	}
}

// DONE
func notifyByzantine(server int, byzantineServers []int) {
	// Set up a connection to the server.
	conn, err := grpc.NewClient(fmt.Sprintf("localhost: %d", 50050+server), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := twoPC.NewTwoPCClient(conn)

	_, err = c.NotifyByzantine(context.Background(), &twoPC.Byzantine{Byzantine: slices.Contains(byzantineServers, server)})
	if err != nil {
		log.Fatalf("could not send transaction to server: %v", err)
	}
}

// DONE
func (c *Client) signTransaction(tx *Transaction) []byte {

	// Create hash
	data, err := json.Marshal(struct {
		S   int
		R   int
		Amt int
	}{tx.S, tx.R, tx.Amt})
	if err != nil {
		return nil
	}
	hash := sha256.Sum256(data)

	// Sign the message hash
	r, s, err := ecdsa.Sign(rand.Reader, c.privateKey, hash[:])
	if err != nil {
		return nil
	}

	// Combine r and s into signature
	signature := append(r.Bytes(), s.Bytes()...)
	return signature
}

func (c *Client) sendTransaction(tx Transaction, ts int) {

	contact := c.txSet.ContactServers[c.shardForID(tx.S)]
	server := c.serverConns[contact]

	// Signing, timestamping
	signature := c.signTransaction(&tx)
	if signature == nil {
		log.Fatalf("could not generate private key")
	}
	txReq := &twoPC.Transaction{S: strconv.Itoa(tx.S), R: strconv.Itoa(tx.R), Amt: strconv.Itoa(tx.Amt)}

	if c.isIntraShard(tx.S, tx.R) {
		fmt.Printf("Initiating intra-shard tx: %v with ts %d\n", tx, ts)
		_, err := server.RequestTransaction(context.Background(), &twoPC.TxRequest{Tx: txReq, Timestamp: int64(ts), Signature: signature})
		if err != nil {
			log.Fatalf("could not send transaction to server: %v", err)
		}
		c.timers[ts] = time.NewTimer(10 * time.Second)
	} else {
		fmt.Printf("Initiating cross-shard tx with ts %d: %v\n", ts, tx)
		_, err := server.CoordinatorPrepare(context.Background(), &twoPC.TxRequest{Tx: txReq, Timestamp: int64(ts), Signature: signature})
		if err != nil {
			log.Fatalf("could not send transaction to server: %v", err)
		}
		c.timers[ts] = time.NewTimer(40 * time.Second)
	}
	go func() {
		<-c.timers[ts].C
		if c.repliesReceived[ts] < 2 {
			fmt.Printf("Timer ran out for ts %d, resending request to all replicas\n", ts)
			go c.sendTransaction(tx, ts)
		}
	}()
}

func (c *Client) PrintBalance() {
	var in int
	fmt.Printf("Which client? 1,30,510...3000?\n")
	fmt.Scan(&in)

	shard := c.shardForID(in)
	for _, d := range c.clusters[shard] {
		// Set up a connection to the server.
		conn, err := grpc.NewClient(fmt.Sprintf("localhost: %d", 50050+d), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()
		server := twoPC.NewTwoPCClient(conn)

		resp, err := server.RequestBalance(context.Background(), &twoPC.BalanceRequest{ClientID: fmt.Sprintf("%d", in)})
		if err != nil {
			log.Fatalf("could not send transaction to server: %v", err)
		}
		balance, err := strconv.Atoi(resp.Balance)
		if err != nil {
			log.Fatalf("could not convert balance to int: %v", err)
		}
		fmt.Printf("The balance of client %d at server %d is %d\n", in, d, balance)
	}
}

func PrintDS() {
	var in int
	fmt.Printf("Which server? 1, 2, 3, 4, 5, 6, 7, 8, 9? ")
	fmt.Scan(&in)
	// Set up a connection to the server(1 by default but all should have the same DB)
	conn, err := grpc.NewClient(fmt.Sprintf("localhost: %d", 50050+in), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := twoPC.NewTwoPCClient(conn)

	resp, err := c.RequestDS(context.Background(), &twoPC.ServerRequested{ServerID: string(in)})
	if err != nil {
		log.Fatalf("could not send transaction to server: %v\n", err)
	}
	fmt.Printf("Server %d Datastore:\n", in)
	for _, entry := range resp.DS {
		fmt.Printf("[<%d>, %s (%v)]\n", entry.Seq, entry.Status, entry.Tx)
	}
}

// -----------------------------SERVER TO CLIENT-------------------------------------
// SETUP CALLBACK FOR SERVERS TO CALL CLIENT AND RESPOND AND UPDATE CLIENT
func (c *Client) ReplyToClient(_ context.Context, in *twoPC.Reply) (*twoPC.Empty, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ts := int(in.Timestamp)
	c.repliesReceived[ts]++
	if c.repliesReceived[ts] >= 2 {
		stopped := c.timers[ts].Stop()
		if stopped {
			fmt.Printf("Stopped timer as got enough replies for tx with ts %d\n", in.Timestamp)
		}
	}
	return &twoPC.Empty{}, nil
}

// -----------------------------MAIN PROGRAM-------------------------------------

// DONE
// Set up a connection to the servers
func (c *Client) connectServers() {
	for i := 1; i <= 12; i++ {
		conn, err := grpc.NewClient(fmt.Sprintf("localhost: %d", 50050+i), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		serv := twoPC.NewTwoPCClient(conn)
		c.serverConns[i] = serv
	}
}

// DONE
func setupClient(numClusters, clusterSize int) *Client {
	client := &Client{
		numClusters:     numClusters,
		clusterSize:     clusterSize,
		clusters:        make([][]int, numClusters),
		timers:          make(map[int]*time.Timer),
		repliesReceived: make(map[int]int),
		serverConns:     make(map[int]twoPC.TwoPCClient),
	}

	// Connect client as a twoPCServer for servers to contact with
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", "40040"))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	clientServer := grpc.NewServer()
	twoPC.RegisterTwoPCServer(clientServer, client)
	go clientServer.Serve(lis)

	client.connectServers()

	// Generate ECDSA key pair for signing
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("couldn't generate private key:%v\n", err)
	}
	client.privateKey = privateKey

	// Fill in server names
	for i := 0; i < numClusters; i++ {
		clusterNodes := make([]int, clusterSize)
		for j := 0; j < clusterSize; j++ {
			clusterNodes[j] = i*clusterSize + j + 1
		}
		client.clusters[i] = clusterNodes
	}

	return client
}

func main() {
	txSets := parse(inputFile)
	var clusters = 3
	var clusterSize = 4
	// Setup client
	c := setupClient(clusters, clusterSize)

	for _, set := range txSets {
		fmt.Printf("Set %d: Live Servers %v Byzantine Servers: %v Contact Servers: %v\n", set.Num, set.LiveServers, set.ByzantineServers, set.ContactServers)

		fmt.Println("ENTER to begin processing set: ")
		fmt.Scanln()
		c.txSet = set

		// Notify servers of their liveness
		for i := 1; i <= (c.numClusters * c.clusterSize); i++ {
			notifyLiveness(i, set.LiveServers)
		}
		for i := 1; i <= (c.numClusters * c.clusterSize); i++ {
			notifyByzantine(i, set.ByzantineServers)
		}
		// Process set once prompted
		for _, tx := range set.Transactions {
			c.timestamp++
			c.sendTransaction(tx, c.timestamp)
		}

		// Once done, allow printDB, printLog, etc.
		var in string
		i := 0
	input:
		for i < 1 {
			fmt.Println("Print [b]alance, [d]atabase, or [c]ontinue:")
			fmt.Scan(&in)
			switch strings.ToUpper(in) {
			case "B":
				c.PrintBalance()
			case "D":
				PrintDS()
			case "C":
				break input
			default:
				fmt.Println("Wrong input type. Try again.")
			}
		}
	}

	fmt.Printf("Finished processing all transaction sets. Exiting..")
	os.Exit(0)
}
