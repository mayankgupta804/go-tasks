package main

import (
	"log"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	ID           string
	IsCompleted  bool
	Status       string
	TimeValidity time.Time
}

const taskChannelBufferSize = 10

func main() {
	tasks := make(chan Task, taskChannelBufferSize)

	wg := &sync.WaitGroup{}

	log.Println("Starting process...")
	wg.Add(1)
	go CheckTasksCompletion(wg, tasks)

	PushTasksIntoQueue(tasks, taskChannelBufferSize)

	wg.Wait()
	log.Println("Task processing over")
}

func CheckTasksCompletion(wg *sync.WaitGroup, tasks chan Task) {
	ticker := time.NewTicker(5 * time.Second)
	quit := make(chan struct{})
	for {
		select {
		case <-ticker.C:
			checkTasksAtIntervals(tasks, quit)
		case <-quit:
			log.Println("Quitting tasks completion checker...")
			ticker.Stop()
			wg.Done()
			return
		}
	}
}

func checkTasksAtIntervals(tasks chan Task, quit chan struct{}) {
	for task := range tasks {
		taskID := task.ID
		switch task.IsCompleted {
		case true:
			log.Printf("task id: %s got completed", taskID)
		case false:
			log.Printf("task id: %s did not get completed", taskID)
			log.Printf("task id: %s checking if timeout has been exceeded", taskID)
			if time.Now().After(task.TimeValidity) {
				log.Printf("task id: %s timeout exceeded", taskID)
				task.Status = "timeout"
			} else {
				log.Printf("task id: %s requeing", taskID)
				tasks <- task
			}
		}
		if len(tasks) == 0 {
			close(tasks)
		}
	}
	log.Println("Processed all tasks...")
	close(quit)
}

func PushTasksIntoQueue(tasks chan<- Task, numOfTasks int) {
	for i := 0; i < numOfTasks; i++ {
		tasks <- Task{
			ID: strconv.Itoa(i + 1),
			IsCompleted: func(currIdx int) bool {
				if currIdx%2 == 0 {
					return true
				}
				return false
			}(i),
			Status: func(currIdx int) string {
				if currIdx%2 == 0 {
					return "completed"
				}
				return ""
			}(i),
			TimeValidity: func(currIdx int) time.Time {
				if currIdx%2 == 0 {
					return time.Now()
				}
				return time.Now().Add(time.Duration((i+1)*1) * time.Second)
			}(i),
		}
	}
}
