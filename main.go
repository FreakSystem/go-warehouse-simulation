package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	producerTick = 4
	consumerTick = 3
	reportTick   = 10
)

type Event struct {
	Type    string
	Product string
	Amount  int
	Success bool
}

type Warehouse struct {
	mu    sync.Mutex
	cola  int
	water int
	fanta int
}

func (w *Warehouse) addCola(n int, eventCh chan Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cola += n
	eventCh <- Event{Type: "add", Product: "cola", Amount: n, Success: true}
}

func (w *Warehouse) addWater(n int, eventCh chan Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.water += n
	eventCh <- Event{Type: "add", Product: "water", Amount: n, Success: true}
}

func (w *Warehouse) addFanta(n int, eventCh chan Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fanta += n
	eventCh <- Event{Type: "add", Product: "fanta", Amount: n, Success: true}
}

func (w *Warehouse) buyCola(eventCh chan Event) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cola > 0 {
		w.cola--

		eventCh <- Event{Type: "buy", Product: "cola", Amount: 1, Success: true}
		return true
	}
	eventCh <- Event{Type: "buy", Product: "cola", Amount: 1, Success: false}
	return false
}

func (w *Warehouse) buyWater(eventCh chan Event) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.water > 0 {
		w.water--

		eventCh <- Event{Type: "buy", Product: "water", Amount: 1, Success: true}
		return true
	}
	eventCh <- Event{Type: "buy", Product: "water", Amount: 1, Success: false}
	return false
}

func (w *Warehouse) buyFanta(eventCh chan Event) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fanta > 0 {
		w.fanta--

		eventCh <- Event{Type: "buy", Product: "fanta", Amount: 1, Success: true}
		return true
	}
	eventCh <- Event{Type: "buy", Product: "fanta", Amount: 1, Success: false}
	return false
}

func producer(w *Warehouse, producerch chan bool, eventCh chan Event, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var timemaneger int
	var amount int

	for {
		select {
		case <-producerch:
			timemaneger++
			if timemaneger%producerTick == 0 {
				amount = rand.IntN(5) + 1
				w.addCola(amount, eventCh)
				amount = rand.IntN(5) + 1
				w.addWater(amount, eventCh)
				amount = rand.IntN(5) + 1
				w.addFanta(amount, eventCh)
			}
		case <-stop:
			fmt.Println("producer остановлен")
			return
		}
	}
}

func consumer(w *Warehouse, consumerch chan bool, eventCh chan Event, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var timemaneger int
	var randbuy int

	for {
		select {
		case <-consumerch:
			timemaneger++
			if timemaneger%consumerTick == 0 {
				randbuy = rand.IntN(3)
				switch randbuy {
				case 0:
					w.buyCola(eventCh)
				case 1:
					w.buyWater(eventCh)
				case 2:
					w.buyFanta(eventCh)
				}
			}
		case <-stop:
			fmt.Println("consumer остановлен")
			return
		}
	}
}

func inspector(w *Warehouse, inspectorch chan bool, eventCh chan Event, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var timemaneger int
	var e Event
	var totalAdd, trueBuy, falseBuy int

	for {
		select {
		case <-inspectorch:
			timemaneger++

			if timemaneger%reportTick == 0 {
				w.mu.Lock()
				fmt.Printf("Отчёт: +%d, куплено %d, неудач %d, склад: кола=%d, вода=%d, фанта=%d\n",
					totalAdd, trueBuy, falseBuy, w.cola, w.water, w.fanta)
				w.mu.Unlock()
			}

		case e = <-eventCh:
			switch e.Type {
			case "add":
				totalAdd += e.Amount
			case "buy":
				if e.Success == true {
					trueBuy++
				} else {
					falseBuy++
				}
			}
		case <-stop:
			fmt.Println("inspector остановлен")
			return
		}
	}
}

func asynctime(producerch chan bool, consumerch chan bool, inspectorch chan bool, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			producerch <- true
			consumerch <- true
			inspectorch <- true
		case <-stop:
			fmt.Println("asynctime остановлен")
			return
		}
	}
}

func main() {
	stop := make(chan struct{})
	var wg sync.WaitGroup

	producerch := make(chan bool, 1)
	consumerch := make(chan bool, 1)
	inspectorch := make(chan bool, 1)

	eventCh := make(chan Event, 100)

	storage := &Warehouse{}

	wg.Add(1)
	go asynctime(producerch, consumerch, inspectorch, stop, &wg)
	wg.Add(1)
	go producer(storage, producerch, eventCh, stop, &wg)
	wg.Add(1)
	go consumer(storage, consumerch, eventCh, stop, &wg)
	wg.Add(1)
	go inspector(storage, inspectorch, eventCh, stop, &wg)

	time.Sleep(time.Second * 30)
	close(stop)
	wg.Wait()
	fmt.Println("Программа завершена корректно")
}
