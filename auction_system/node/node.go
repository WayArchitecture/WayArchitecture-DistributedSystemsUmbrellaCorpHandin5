package main

import (
	pb "auction_system/grpc"
	"context"
	"fmt"
	"maps"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeStruct struct {
	pbNode *pb.Node
	client pb.AuctionServiceClient
	conn   *grpc.ClientConn
}

type Server struct {
	pb.UnimplementedAuctionServiceServer
	myPbNode   *pb.Node
	leaderNode *NodeStruct
	nodes      map[int]NodeStruct

	clients    []*pb.Client
	highestBid int64

	electionOngoing   bool
	electionNumber    int32
	electionCandidate int32

	auctionEndTime time.Time

	mu sync.Mutex
}

func main() {
	var MAX_NB_NODES = 5

	server := Server{
		nodes:             make(map[int]NodeStruct),
		clients:           []*pb.Client{},
		electionOngoing:   false,
		electionNumber:    0,
		electionCandidate: -1,
	}

	//find what port (and id) we can use
	for i := range MAX_NB_NODES {
		address := "localhost:505" + strconv.Itoa(i)
		//fmt.Println("checking: " + address)
		tmpListener, err := net.Listen("tcp", address)
		if err != nil {
			fmt.Println(address + " is already in use")

		} else {
			server.myPbNode = &pb.Node{
				Id:       int32(i),
				Address:  address,
				IsLeader: false,
			}
			fmt.Println("using: " + address)
			tmpListener.Close()
			break
		}
	}

	isLeader := true //flag used to check if we are the leader
	//create clients for existing nodes and set the listener for our own server
	var listener net.Listener
	for i := range MAX_NB_NODES {
		address := "localhost:505" + strconv.Itoa(i)
		//fmt.Println("checking: " + address)
		tmpListener, err := net.Listen("tcp", address)
		if err != nil {
			isLeader = false
			conn, _ := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

			client := pb.NewAuctionServiceClient(conn)
			var pbNode, _ = client.RegisterBackupNode(context.Background(), server.myPbNode)

			node := NodeStruct{
				pbNode: pbNode,
				client: client,
				conn:   conn,
			}

			server.nodes[i] = node

			if node.pbNode.IsLeader {
				server.leaderNode = &node
			}

		} else if server.myPbNode.Id == int32(i) {
			listener = tmpListener
			//fmt.Println("using: " + address)

		} else {
			tmpListener.Close()
			//fmt.Println(address + " is not used")
		}
	}

	if listener == nil {
		fmt.Println("All nodes are in use. Aborting...")
		return
	}

	server.myPbNode.IsLeader = isLeader

	if isLeader {
		server.auctionEndTime = time.Now().Add(100 * time.Second)

		fmt.Println("Auction started!")
		fmt.Println("Auction deadline:", server.auctionEndTime)

	}
	//fmt.Println("server.myNode.IsLeader: ", server.myNode.IsLeader)

	grpcServer := grpc.NewServer()

	pb.RegisterAuctionServiceServer(grpcServer, &server)

	if server.leaderNode != nil {
		startPingingLeaderNode(&server)
	}

	//app termination listener
	closeChannel := make(chan os.Signal, 1)

	signal.Notify(
		closeChannel,
		os.Interrupt,    // Ctrl+C
		syscall.SIGTERM, // window close
	)

	go func() {
		<-closeChannel

		//disconnectMe(server)
		grpcServer.Stop()
	}()

	if err := grpcServer.Serve(listener); err != nil {
		//disconnectMe(server)
		grpcServer.Stop()
	}
}

func (s *Server) RegisterBackupNode(ctx context.Context, req *pb.Node) (*pb.Node, error) {
	//create client to use when updating the new node
	conn, _ := grpc.NewClient(req.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	client := pb.NewAuctionServiceClient(conn)

	node := NodeStruct{
		pbNode: req,
		client: client,
		conn:   conn,
	}
	s.mu.Lock()
	s.nodes[int(req.Id)] = node
	s.mu.Unlock()

	return s.myPbNode, nil
}

func startPingingLeaderNode(s *Server) {
	go func() {
		for {
			time.Sleep(3 * time.Second)
			fmt.Println("ping from backup...")

			s.mu.Lock()
			leader := s.leaderNode
			if leader == nil || leader.client == nil {
				s.mu.Unlock()
				return
			}

			leaderID := leader.pbNode.Id
			leaderClient := leader.client
			s.mu.Unlock()

			// Tell the grpc call to stop after 3 seconds
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err := leaderClient.PingPrimaryNodeFromBackup(ctx, &pb.Nothing{})

			//Dont wait 1 seconds if a ping arrives
			cancel()

			if err != nil {
				fmt.Println("Primary node is unreachable:", err)
				// Only start an election if this is still our leader.
				s.mu.Lock()
				stillSameLeader := s.leaderNode != nil &&
					s.leaderNode.pbNode.Id == leaderID
				s.mu.Unlock()

				if stillSameLeader {
					s.startElectionIfNeeded()
				}
				return
			}
		}
	}()
}

func (s *Server) PingPrimaryNodeFromBackup(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	fmt.Println("ping from leader...")
	return &pb.Nothing{}, nil
}

func (s *Server) RegisterClient(ctx context.Context, req *pb.Nothing) (*pb.Node, error) {
	if s.isElectionOngoing() {
		return nil, fmt.Errorf("Election ongoing")
	}
	if s.leaderNode != nil {
		return s.leaderNode.pbNode, nil
	}

	return s.myPbNode, nil
}

func (s *Server) Bid(ctx context.Context, req *pb.Amount) (*pb.Acknowledgement, error) {
	fmt.Println("Bid... Amount:", req.Amount)

	s.mu.Lock()

	//Check whether the auction deadline exists
	if s.auctionEndTime.IsZero() {
		s.mu.Unlock()
		return &pb.Acknowledgement{
			Status: "Exception",
		}, nil

	}

	//Checks whether the auction has ended
	if !time.Now().Before(s.auctionEndTime) {
		s.mu.Unlock()

		fmt.Println("Auction has ended!")

		return &pb.Acknowledgement{
			Status: "Fail",
		}, nil
	}
	//Check if bid is valid
	if req.Amount < s.highestBid || req.Amount == s.highestBid {
		s.mu.Unlock()
		return &pb.Acknowledgement{Status: "Fail"}, nil
	}

	//Update backup nodes
	state := &pb.PrimaryState{HighestBid: req.Amount, AuctionEndTime: 0, UniqueIdentifier: s.myPbNode.Id}

	for _, element := range s.nodes {
		newCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		element.client.PrimaryUpdateToBackup(newCtx, state)

		cancel()
	}

	s.highestBid = req.Amount

	s.mu.Unlock()

	ack := pb.Acknowledgement{
		Status: "Success",
	}

	return &ack, nil
}

func (s *Server) Result(ctx context.Context, req *pb.Nothing) (*pb.Outcome, error) {
	fmt.Println("Result...")

	s.mu.Lock()

	ack := pb.Outcome{
		HighestBid: s.highestBid,
		IsOver:     false,
	}

	s.mu.Unlock()

	return &ack, nil
}

func (s *Server) PrimaryUpdateToBackup(ctx context.Context, state *pb.PrimaryState) (*pb.Nothing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.highestBid = state.HighestBid
	// Update auctionEndTime

	return &pb.Nothing{}, nil
}

func (s *Server) startElectionIfNeeded() {
	s.mu.Lock()

	if s.electionOngoing {
		s.mu.Unlock()
		return
	}

	s.electionNumber++
	s.electionOngoing = true
	s.electionCandidate = s.myPbNode.Id

	candidate := &pb.Candidate{
		Id:             s.myPbNode.Id,
		ElectionNumber: s.electionNumber,
	}

	s.mu.Unlock()

	fmt.Println("Starting election, term:", candidate.ElectionNumber)
	go func() {
		_, err := s.StartElection(context.Background(), candidate)
		if err != nil {
			fmt.Println("Election failed:", err)
		}
	}()
}

func (s *Server) StartElection(ctx context.Context, candidate *pb.Candidate) (*pb.Acknowledgement, error) {
	fmt.Println("Recieved request to start election")
	s.mu.Lock()
	if candidate.ElectionNumber < s.electionNumber {
		s.mu.Unlock()
		return &pb.Acknowledgement{Status: "Denied"}, nil
	}

	if candidate.ElectionNumber > s.electionNumber {
		s.electionNumber = candidate.ElectionNumber
		s.electionOngoing = true
		s.electionCandidate = candidate.Id
	} else {
		if !s.electionOngoing {
			s.electionOngoing = true
			s.electionCandidate = candidate.Id
		} else if candidate.Id < s.electionCandidate {
			s.electionCandidate = candidate.Id
		}
	}

	selectedCandidate := s.electionCandidate

	nodes := make(map[int]NodeStruct)
	maps.Copy(nodes, s.nodes)

	s.mu.Unlock()

	if len(nodes) == 0 {
		fmt.Println("No other nodes available. Electing myself.")

		s.ElectLeader(ctx, candidate)

		return &pb.Acknowledgement{Status: "success"}, nil
	}

	if candidate.Id != s.myPbNode.Id || selectedCandidate != candidate.Id {
		return &pb.Acknowledgement{Status: "Accepted"}, nil
	}

	//Find responsive nodes
	responsiveNodes := make(map[int]NodeStruct)

	for id, element := range nodes {

		newCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ack, err := element.client.StartElection(newCtx, candidate)

		cancel()

		if err != nil || ack == nil {
			fmt.Println("Node", id, "did not respond")
			continue
		}

		responsiveNodes[id] = element
	}

	//Start ping from candidates to check whether election handler still lives
	for _, element := range responsiveNodes {
		newCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		element.client.PingElectionHandler(newCtx, s.myPbNode)
		cancel()
	}

	//Compare candidates to find leader
	for _, element := range responsiveNodes {
		newCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		priority, err := element.client.CompareCandidate(newCtx, candidate)

		cancel()
		if err == nil && priority != nil && priority.Id < candidate.Id {
			candidate = priority
		}
	}

	//Compare state
	s.mu.Lock()
	state := &pb.ElectionState{HighestBid: s.highestBid, AuctionEndTime: 0}
	s.mu.Unlock()

	for _, element := range responsiveNodes {
		newCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		priority, err := element.client.CompareState(newCtx, state)

		//Dont wait 1 seconds if a ping arrives
		cancel()
		if err == nil && priority != nil && priority.HighestBid > state.HighestBid {
			state = priority
		}
	}

	//Update states of all nodes
	primaryState := &pb.PrimaryState{HighestBid: state.HighestBid, AuctionEndTime: state.AuctionEndTime, UniqueIdentifier: candidate.Id}
	s.PrimaryUpdateToBackup(ctx, primaryState)

	committedNodes := make(map[int]NodeStruct)

	for id, element := range responsiveNodes {
		rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := element.client.PrimaryUpdateToBackup(rpcCtx, primaryState)
		cancel()

		if err == nil {
			committedNodes[id] = element
		}
	}

	//Elect new leader and end election
	for _, element := range committedNodes {
		rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := element.client.ElectLeader(rpcCtx, candidate)
		cancel()

		if err != nil {
			fmt.Println("Could not notify node of new leader:", err)
		}
	}

	s.ElectLeader(ctx, candidate)

	fmt.Println("Election complete")

	return &pb.Acknowledgement{}, nil
}

func (s *Server) PingElectionHandler(ctx context.Context, handler *pb.Node) (*pb.Nothing, error) {
	go func() {
		for s.isElectionOngoing() {
			time.Sleep(3 * time.Second)
			fmt.Println("pinging election handler...")

			// Tell the grpc call to stop after 3 seconds
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err := s.nodes[int(handler.Id)].client.ReceivePing(ctx, &pb.Nothing{})

			//Dont wait 1 seconds if a ping arrives
			cancel()

			if err != nil {
				fmt.Println("Handler is unreachable:", err)
				// Start election here
				candidate := pb.Candidate{Id: s.myPbNode.Id, ElectionNumber: s.electionNumber + 1}
				fmt.Println("Started election")
				s.StartElection(context.Background(), &candidate)
				break
			}
		}
	}()
	return &pb.Nothing{}, nil
}

func (s *Server) ReceivePing(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	fmt.Println("Recieved ping")
	return &pb.Nothing{}, nil
}

func (s *Server) CompareCandidate(ctx context.Context, candidate *pb.Candidate) (*pb.Candidate, error) {
	//Compare id priority
	s.mu.Lock()
	defer s.mu.Unlock()

	if candidate.ElectionNumber != s.electionNumber {
		return nil, fmt.Errorf("election term mismatch")
	}

	id := candidate.Id

	if s.myPbNode.Id < id {
		id = s.myPbNode.Id
	}

	return &pb.Candidate{Id: id, ElectionNumber: candidate.ElectionNumber}, nil
}

func (s *Server) CompareState(ctx context.Context, state *pb.ElectionState) (*pb.ElectionState, error) {
	//Compare highest bid
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.highestBid > state.HighestBid {
		return &pb.ElectionState{
			HighestBid:     s.highestBid,
			AuctionEndTime: state.AuctionEndTime,
		}, nil
	}

	return state, nil
}

func (s *Server) ElectLeader(ctx context.Context, candidate *pb.Candidate) (*pb.Nothing, error) {
	s.mu.Lock()

	if candidate.ElectionNumber < s.electionNumber {
		s.mu.Unlock()
		return nil, fmt.Errorf("Leader from old election")
	}

	if candidate.ElectionNumber > s.electionNumber {
		s.electionNumber = candidate.ElectionNumber
	}

	if candidate.Id == s.myPbNode.Id {
		// This node is the new leader.
		s.myPbNode.IsLeader = true
		s.leaderNode = nil
	} else {
		// This node becomes a backup.
		node, exists := s.nodes[int(candidate.Id)]
		if !exists {
			s.mu.Unlock()
			return nil, fmt.Errorf(
				"new leader %d is not registered",
				candidate.Id,
			)
		}

		s.myPbNode.IsLeader = false
		s.leaderNode = &node
	}

	s.electionCandidate = candidate.Id
	s.electionOngoing = false

	isBackup := candidate.Id != s.myPbNode.Id

	s.mu.Unlock()

	if isBackup {
		startPingingLeaderNode(s)
	}

	return &pb.Nothing{}, nil
}

func (s *Server) isElectionOngoing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.electionOngoing
}
