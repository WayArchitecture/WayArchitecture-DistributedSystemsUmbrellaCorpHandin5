package main

import (
	pb "auction_system/grpc"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeStruct struct {
	pbNode *pb.Node
	client pb.AuctionServiceClient
}

type Server struct {
	pb.UnimplementedAuctionServiceServer
	myPbNode   *pb.Node
	leaderNode *NodeStruct
	nodes      map[int]NodeStruct
	clients    []*pb.Client
}

func main() {
	var MAX_NB_NODES = 3

	server := Server{
		nodes:   make(map[int]NodeStruct),
		clients: []*pb.Client{},
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
			defer conn.Close()

			client := pb.NewAuctionServiceClient(conn)
			var pbNode, _ = client.RegisterBackupNode(context.Background(), server.myPbNode)

			node := NodeStruct{
				pbNode: pbNode,
				client: client,
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
	defer conn.Close()

	client := pb.NewAuctionServiceClient(conn)

	node := NodeStruct{
		pbNode: req,
		client: client,
	}
	s.nodes[int(req.Id)] = node

	return s.myPbNode, nil
}

func startPingingLeaderNode(s *Server) {
	go func() {
		for {
			time.Sleep(3 * time.Second)
			fmt.Println("ping from backup...")
			s.leaderNode.client.PingPrimaryNodeFromBackup(context.Background(), &pb.Nothing{})
		}
	}()
}

func (s *Server) PingPrimaryNodeFromBackup(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	fmt.Println("ping from leader...")
	return &pb.Nothing{}, nil
}
