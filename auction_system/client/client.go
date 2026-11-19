package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auction_system/grpc"
)

func main() {
	conn, _ := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(
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
	fmt.Println(students)
}
