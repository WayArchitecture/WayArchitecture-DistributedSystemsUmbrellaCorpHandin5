package main

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auction_system/grpc"
)

type Client struct {
	client       pb.AuctionServiceClient
	leaderNode   *pb.Node
	conn         *grpc.ClientConn
	MAX_NB_NODES int
	clientId     int
}

func main() {
	/*conn, _ := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(
		insecure.NewCredentials()))

	client := pb.NewITUServiceClient(conn)

	students, _ := client.GetStudents(context.Background(), &pb.Nothing{})
	fmt.Println(students)

	new_student := pb.Student{
		Name: "eve",
		Age:  32,
	}

	client.AddStudent(context.Background(), &new_student)

	students, _ = client.GetStudents(context.Background(), &pb.Nothing{})
	fmt.Println(students)*/

	var MAX_NB_NODES = 5
	//var MAX_NB_CLIENTS = 10
	var client pb.AuctionServiceClient
	nodeFound := false
	var leaderNode *pb.Node
	var err error
	var conn *grpc.ClientConn
	//find a node
	for i := range MAX_NB_NODES {
		address := "localhost:505" + strconv.Itoa(i)
		fmt.Println("checking: " + address)
		conn, _ = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

		client = pb.NewAuctionServiceClient(conn)

		leaderNode, err = client.RegisterClient(context.Background(), &pb.Nothing{})

		if err == nil {
			nodeFound = true
			if leaderNode.Id != int32(i) {
				conn, _ = grpc.NewClient(leaderNode.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				client = pb.NewAuctionServiceClient(conn)
				defer conn.Close()
			}
			break
		} else {
			conn.Close()
		}
	}

	if nodeFound == false {
		fmt.Println("No leader node found. Aborting...")
		return
	}

	fmt.Println("Leader node found... id:", leaderNode.Id)

	Client := Client{
		leaderNode:   leaderNode,
		client:       client,
		conn:         conn,
		MAX_NB_NODES: MAX_NB_NODES,
		clientId:     -1,
	}

	fmt.Println("To place a bid, type 'Bid' followed by the amount you want to bid.")
	fmt.Println("'Bid amount'")

	fmt.Println("To see current statues of the auction, type 'Result'.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Action: ")

		if !scanner.Scan() || scanner.Err() != nil {
			break
		}

		var input = strings.Split(scanner.Text(), " ")
		switch strings.ToLower(input[0]) {
		case "bid":
			if len(input) != 2 {
				fmt.Println("Please input the bid request correctly, in the following format.")
				fmt.Println("'Bid amount'")
				continue
			}
			bidAmount, err := strconv.ParseInt(input[1], 10, 64)

			if err != nil {
				fmt.Println(err)
				continue
			}
			//Bid
			bidErr := PlaceBid(context.Background(), &Client, bidAmount)

			if bidErr != nil {
				return
			}

			resErr := ShowResult(context.Background(), &Client)

			if resErr != nil {
				return
			}

		case "result":
			if len(input) != 1 {
				fmt.Println("Please only type 'Result' when viewing results.")
			}

			resErr := ShowResult(context.Background(), &Client)

			if resErr != nil {
				return
			}

		default:
			fmt.Println("The given action wasn't found.")
		}
	}
}

func PlaceBid(ctx context.Context, client *Client, amount int64) error {
	bidReq := &pb.BidRequest{
		Amount:   amount,
		ClientId: int64(client.clientId),
	}

	bidCtx, bidCancel := context.WithTimeout(ctx, 3*time.Second)
	//Bid
	var ack, bidErr = client.client.Bid(bidCtx, bidReq)

	bidCancel()

	if bidErr != nil {
		fmt.Println("Primary node is unreachable:", bidErr)
		newLeader, newConn, findErr := FindLeader(client.MAX_NB_NODES)
		if findErr == nil {
			SetNewLeader(newLeader, newConn, client)
		} else {
			fmt.Print(findErr)
			return findErr
		}
	}

	if ack != nil {
		client.clientId = int(ack.ClientId)
		switch ack.Status {
		case "Fail: Too low":
			fmt.Println("Bid was not higher than the current highest.")
		case "Success":
			fmt.Println("Successfully placed bid.")
		}
	}
	return nil
}

func ShowResult(ctx context.Context, client *Client) error {
	resultCtx, resultCancel := context.WithTimeout(ctx, 3*time.Second)

	var outcome, resultErr = client.client.Result(resultCtx, &pb.Nothing{})

	resultCancel()

	if resultErr != nil {
		fmt.Println("Primary node is unreachable:", resultErr)
		newLeader, newConn, findErr := FindLeader(client.MAX_NB_NODES)
		if findErr == nil {
			SetNewLeader(newLeader, newConn, client)
		} else {
			fmt.Print(findErr)
			return findErr
		}
	}

	if outcome != nil {
		timeleft := int64(outcome.AuctionEndTime) - time.Now().Unix()
		if !outcome.IsOver {
			fmt.Println("Current highest bid:", outcome.HighestBid)
			fmt.Println("Auction ends in", timeleft, "seconds.")
		} else {
			fmt.Println("Auction has ended.")
			fmt.Println("Auction ended", math.Abs(float64(timeleft)), "seconds ago.")
			fmt.Println("The highest bid was", outcome.HighestBid)
		}
	}
	return nil
}

func SetNewLeader(newLeader *pb.Node, newConn *grpc.ClientConn, client *Client) {
	client.leaderNode = newLeader
	client.client = pb.NewAuctionServiceClient(newConn)
	oldConn := client.conn
	client.conn = newConn

	if oldConn != nil {
		oldConn.Close()
	}
}

func FindLeader(MAX_NB_NODES int) (*pb.Node, *grpc.ClientConn, error) {
	for range 5 {
		fmt.Println("Looking for new leader...")

		for i := range MAX_NB_NODES {
			address := "localhost:505" + strconv.Itoa(i)
			fmt.Println("checking: " + address)
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

			if err != nil {
				continue
			}

			nodeClient := pb.NewAuctionServiceClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			leaderNode, err := nodeClient.RegisterClient(ctx, &pb.Nothing{})
			cancel()

			conn.Close()

			if err != nil {
				fmt.Println(err)
				continue
			}
			if leaderNode == nil {
				fmt.Println("Leader node not set in current node")
				continue
			}

			leaderConn, err := grpc.NewClient(
				leaderNode.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				continue
			}

			// Verify that leader is actually reachable
			leaderClient := pb.NewAuctionServiceClient(leaderConn)

			verifyCtx, verifyCancel := context.WithTimeout(
				context.Background(), 3*time.Second,
			)
			_, err = leaderClient.Result(verifyCtx, &pb.Nothing{})
			verifyCancel()

			if err == nil {
				return leaderNode, leaderConn, nil
			}

			leaderConn.Close()
		}

		time.Sleep(3 * time.Second)
	}
	return nil, nil, fmt.Errorf("Could not find a leader after 3 attempts")
}
