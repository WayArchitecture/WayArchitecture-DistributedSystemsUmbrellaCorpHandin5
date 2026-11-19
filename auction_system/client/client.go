package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"

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

	var MAX_NB_NODES = 3
	//var MAX_NB_CLIENTS = 10

	var client pb.AuctionServiceClient
	nodeFound := false
	var leaderNode *pb.Node
	var err error
	//find a node
	for i := range MAX_NB_NODES {
		address := "localhost:505" + strconv.Itoa(i)
		fmt.Println("checking: " + address)
		conn, _ := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

		client = pb.NewAuctionServiceClient(conn)

		leaderNode, err = client.RegisterClient(context.Background(), &pb.Nothing{})

		if err == nil {
			nodeFound = true
			if leaderNode.Id != int32(i) {
				conn, _ := grpc.NewClient(leaderNode.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

		var ack, _ = client.Bid(context.Background(), amount)
		fmt.Println("ack:", ack.Status)

		var outcome, _ = client.Result(context.Background(), &pb.Nothing{})
		fmt.Println("outcome:", outcome.HighestBid)
	}
}
