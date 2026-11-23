package main

import (
	pb "auction_system/grpc"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
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
	electionRunning   bool
	electionNumber    int32
	electionCandidate int32

	auctionEndTime time.Time
	mu             sync.Mutex
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

func (s *Server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.Acknowledgement, error) {
	fmt.Println("Bid... Amount:", req.Amount)

	s.mu.Lock()
	if !s.myPbNode.IsLeader || s.electionOngoing {
		s.mu.Unlock()
		return &pb.Acknowledgement{
			Status:   "Fail: Not leader",
			ClientId: req.ClientId,
		}, nil
	}

	id := req.ClientId
	if req.ClientId == -1 {
		id = int64(len(s.clients))
		client := pb.Client{
			HighestBid: 0,
			ClientId:   id,
		}
		s.clients = append(s.clients, &client)
	}

	//Check whether the auction deadline exists
	if s.auctionEndTime.IsZero() {
		s.mu.Unlock()
		return &pb.Acknowledgement{
			Status:   "Exception",
			ClientId: id,
		}, nil

	}

	//Checks whether the auction has ended
	if !time.Now().Before(s.auctionEndTime) {
		s.mu.Unlock()

		fmt.Println("Auction has ended!")

		return &pb.Acknowledgement{
			Status:   "Fail: Over",
			ClientId: id,
		}, nil
	}
	//Check if bid is valid
	if req.Amount < s.highestBid || req.Amount == s.highestBid {
		s.mu.Unlock()
		return &pb.Acknowledgement{Status: "Fail: Too low", ClientId: id}, nil
	}

	//Update backup nodes
	state := &pb.PrimaryState{HighestBid: req.Amount,

		//This now send the same deadline to every backup
		AuctionEndTime: int32(s.auctionEndTime.Unix()),

		UniqueIdentifier: s.myPbNode.Id,

		Clients: &pb.Clients{Clients: s.clients},
	}

	for _, element := range s.nodes {
		newCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		element.client.PrimaryUpdateToBackup(newCtx, state)

		cancel()
	}

	s.highestBid = req.Amount

	s.mu.Unlock()

	ack := pb.Acknowledgement{
		Status:   "Success",
		ClientId: id,
	}

	return &ack, nil
}

func (s *Server) Result(ctx context.Context, req *pb.Nothing) (*pb.Outcome, error) {
	fmt.Println("Result...")

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.auctionEndTime.IsZero() {
		return nil, fmt.Errorf("Auction deadline not initialized")
	}

	isOver := !time.Now().Before(s.auctionEndTime)

	ack := pb.Outcome{
		HighestBid:     s.highestBid,
		IsOver:         isOver,
		AuctionEndTime: int32(s.auctionEndTime.Unix()),
	}

	return &ack, nil
}

func (s *Server) PrimaryUpdateToBackup(ctx context.Context, state *pb.PrimaryState) (*pb.Nothing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.highestBid = state.HighestBid

	if state.AuctionEndTime != 0 {
		s.auctionEndTime = time.Unix(int64(state.AuctionEndTime), 0)
	}

	s.clients = make([]*pb.Client, len(state.Clients.Clients))

	for i, client := range state.Clients.Clients {
		s.clients[i] = proto.Clone(client).(*pb.Client)
	}

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
	s.electionRunning = true
	s.electionCandidate = s.myPbNode.Id

	term := s.electionNumber
	s.mu.Unlock()

	fmt.Println("Starting election, term:", term)

	go s.HandleElection(term)
}

func (s *Server) StartElection(ctx context.Context, candidate *pb.Candidate) (*pb.Acknowledgement, error) {

	s.mu.Lock()

	if candidate.ElectionNumber < s.electionNumber {
		s.mu.Unlock()
		return &pb.Acknowledgement{Status: "Denied"}, nil
	}

	if candidate.ElectionNumber > s.electionNumber {
		s.electionNumber = candidate.ElectionNumber
		s.electionOngoing = true
		s.electionRunning = false
		s.electionCandidate = candidate.Id
	} else {
		s.electionOngoing = true

		if candidate.Id < s.electionCandidate {
			s.electionCandidate = candidate.Id
		}
	}

	shouldRun := s.electionCandidate == s.myPbNode.Id &&
		!s.electionRunning

	term := s.electionNumber

	if shouldRun {
		s.electionRunning = true
	}

	s.mu.Unlock()

	if shouldRun {
		go s.HandleElection(term)
	}

	return &pb.Acknowledgement{Status: "Accepted"}, nil
}

func (s *Server) HandleElection(term int32) {
	defer func() {
		s.mu.Lock()
		if s.electionNumber == term {
			s.electionRunning = false
		}
		s.mu.Unlock()
	}()

	s.mu.Lock()
	if !s.electionOngoing ||
		s.electionNumber != term ||
		s.electionCandidate != s.myPbNode.Id {
		s.mu.Unlock()
		return
	}

	nodes := make(map[int]NodeStruct)
	for id, node := range s.nodes {
		nodes[id] = node
	}
	selfID := s.myPbNode.Id
	s.mu.Unlock()

	// Find reachable nodes and select the lowest ID.
	responsive := make(map[int]NodeStruct)
	selectedID := selfID

	for id, node := range nodes {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)

		_, err := node.client.ReceivePing(ctx, &pb.Nothing{})
		cancel()

		if err == nil {
			responsive[id] = node

			if int32(id) < selectedID {
				selectedID = int32(id)
			}
		}
	}

	fmt.Println("Selected election candidate:", selectedID)

	// Announce the selected candidate to reachable nodes.
	candidate := &pb.Candidate{
		Id:             selectedID,
		ElectionNumber: term,
	}

	accepted := 1 // This node also accepts the candidate.

	for _, node := range responsive {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)

		ack, err := node.client.StartElection(ctx, candidate)
		cancel()

		if err == nil && ack != nil && ack.Status != "Denied" {
			accepted++
		}
	}

	// If another node has the lowest ID, let it coordinate.
	if selectedID != selfID {
		return
	}

	// Ask reachable nodes to monitor this election coordinator.
	for _, node := range responsive {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)

		_, _ = node.client.PingElectionHandler(ctx, s.myPbNode)
		cancel()
	}

	// Require a majority of the configured membership.
	totalNodes := len(nodes) + 1
	quorum := totalNodes/2 + 1

	if accepted < quorum {
		fmt.Println("Election did not receive enough acknowledgements")
		return
	}

	// Collect the latest auction state.
	s.mu.Lock()
	state := &pb.ElectionState{
		HighestBid:     s.highestBid,
		AuctionEndTime: int32(s.auctionEndTime.Unix()),
		Clients: &pb.Clients{
			Clients: make([]*pb.Client, len(s.clients)),
		},
	}

	for i, client := range s.clients {
		state.Clients.Clients[i] =
			proto.Clone(client).(*pb.Client)
	}
	s.mu.Unlock()

	for _, node := range responsive {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)

		newState, err := node.client.CompareState(ctx, state)
		cancel()

		if err == nil && newState != nil {
			state = newState
		}
	}

	// Update the new leader's state.
	primaryState := &pb.PrimaryState{
		HighestBid:       state.HighestBid,
		AuctionEndTime:   state.AuctionEndTime,
		UniqueIdentifier: selectedID,
		Clients:          state.Clients,
	}

	_, err := s.PrimaryUpdateToBackup(
		context.Background(), primaryState,
	)

	if err != nil {
		fmt.Println("Could not update local state:", err)
		return
	}

	// Replicate recovered state to reachable backups.
	updated := 1

	for _, node := range responsive {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)

		_, err := node.client.PrimaryUpdateToBackup(ctx, primaryState)
		cancel()

		if err == nil {
			updated++
		}
	}

	if updated < quorum {
		fmt.Println("Could not replicate state to a majority")
		return
	}

	// Notify nodes that the new leader has been selected.
	confirmed := 1

	for _, node := range responsive {
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)

		_, err := node.client.ElectLeader(ctx, candidate)
		cancel()

		if err == nil {
			confirmed++
		}
	}

	if confirmed < quorum {
		fmt.Println("Not enough nodes confirmed the new leader")
		return
	}

	// Elect ourselves locally.
	_, err = s.ElectLeader(context.Background(), candidate)

	if err != nil {
		fmt.Println("Could not elect myself:", err)
		return
	}

	fmt.Println("Election complete. New leader:", selectedID)
}

func (s *Server) PingElectionHandler(ctx context.Context, handler *pb.Node) (*pb.Nothing, error) {

	go func() {
		for {
			time.Sleep(3 * time.Second)

			s.mu.Lock()

			if !s.electionOngoing ||
				s.electionCandidate != handler.Id {
				s.mu.Unlock()
				return
			}

			node, exists := s.nodes[int(handler.Id)]
			s.mu.Unlock()

			if !exists || node.client == nil {
				break
			}

			pingCtx, cancel := context.WithTimeout(
				context.Background(), 1*time.Second,
			)

			_, err := node.client.ReceivePing(
				pingCtx, &pb.Nothing{},
			)
			cancel()

			if err != nil {
				fmt.Println("Election coordinator failed")

				s.mu.Lock()

				if s.electionOngoing &&
					s.electionCandidate == handler.Id {
					s.electionOngoing = false
					s.electionRunning = false
				}

				s.mu.Unlock()

				s.startElectionIfNeeded()
				return
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
			AuctionEndTime: int32(s.auctionEndTime.Unix()),
		}, nil
	}

	for _, client := range s.clients {
		found := false

		for _, otherClient := range state.Clients.Clients {
			if client.ClientId == otherClient.ClientId {
				found = true

				if client.HighestBid > otherClient.HighestBid {
					otherClient.HighestBid = client.HighestBid
				}

				break
			}
		}

		// Client doesn't exist in the received state
		if !found {
			state.Clients.Clients = append(state.Clients.Clients, client)
		}
	}

	return state, nil
}

func (s *Server) ElectLeader(ctx context.Context, candidate *pb.Candidate) (*pb.Nothing, error) {

	s.mu.Lock()

	if candidate.ElectionNumber != s.electionNumber {
		s.mu.Unlock()
		return nil, fmt.Errorf("Election term mismatch")
	}

	if candidate.Id != s.electionCandidate {
		s.mu.Unlock()
		return nil, fmt.Errorf("Candidate was not selected")
	}

	if candidate.Id == s.myPbNode.Id {
		s.myPbNode.IsLeader = true
		s.leaderNode = nil
	} else {
		node, exists := s.nodes[int(candidate.Id)]

		if !exists {
			s.mu.Unlock()
			return nil, fmt.Errorf("New leader is not registered")
		}

		s.myPbNode.IsLeader = false
		s.leaderNode = &node
	}

	s.electionOngoing = false
	s.electionRunning = false

	isBackup := candidate.Id != s.myPbNode.Id

	s.mu.Unlock()

	if isBackup {
		startPingingLeaderNode(s)
	}

	fmt.Println("New leader:", candidate.Id)

	return &pb.Nothing{}, nil
}

func (s *Server) isElectionOngoing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.electionOngoing
}
