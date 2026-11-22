package main

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auction_system/grpc"
)

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

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Bid: ")

		if !scanner.Scan() || scanner.Err() != nil {
			break
		}

		var input = scanner.Text()
		bidAmount, err := strconv.ParseInt(input, 10, 64)

		if err != nil {
			fmt.Println(err)
			continue
		}

		amount := &pb.Amount{

			Amount: bidAmount,
		}

		bidCtx, bidCancel := context.WithTimeout(context.Background(), 3*time.Second)
		//Bid
		var ack, bidErr = client.Bid(bidCtx, amount)

		bidCancel()

		if bidErr != nil {
			fmt.Println("Primary node is unreachable:", bidErr)
			newLeader, newConn, findErr := FindLeader(MAX_NB_NODES)
			if findErr == nil {
				leaderNode = newLeader
				client = pb.NewAuctionServiceClient(newConn)
				oldConn := conn
				conn = newConn

				if oldConn != nil {
					oldConn.Close()
				}
			} else {
				fmt.Print(findErr)
				return
			}
		}

		if ack != nil {
			switch ack.Status {
			case "Fail: Too low":
				fmt.Println("Bid was not higher than the current highest.")
			case "Success":
				fmt.Println("Successfully places bid.")
			}
		}

		resultCtx, resultCancel := context.WithTimeout(context.Background(), 3*time.Second)

		var outcome, resultErr = client.Result(resultCtx, &pb.Nothing{})

		resultCancel()

		if resultErr != nil {
			fmt.Println("Primary node is unreachable:", resultErr)
			newLeader, newConn, findErr := FindLeader(MAX_NB_NODES)
			if findErr == nil {
				leaderNode = newLeader
				client = pb.NewAuctionServiceClient(newConn)
				oldConn := conn
				conn = newConn

				if oldConn != nil {
					oldConn.Close()
				}
				continue
			} else {
				fmt.Print(findErr)
				return
			}
		}

		if outcome != nil {
			timeleft := int64(outcome.AuctionEndTime) - time.Now().Unix()
			if !outcome.IsOver {
				fmt.Println("Current highest bid:", outcome.HighestBid)
				fmt.Println("Action ends in", timeleft, "seconds.")
			} else {
				fmt.Println("Auction has ended.")
				fmt.Println("Action ended", math.Abs(float64(timeleft)), "seconds ago.")
				fmt.Println("The highest bid was", outcome.HighestBid)
			}
		}
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
