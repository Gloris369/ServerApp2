package ServerApp2

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	tcpAddress = ":9000"
	udpAddress = ":9001"
)

type Client struct {
	conn net.Conn
	mu   sync.Mutex
}

func (c *Client) send(message string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := fmt.Fprintln(c.conn, message)
	return err
}

type Gateway struct {
	tcp net.Listener
	udp net.PacketConn

	mu      sync.RWMutex
	clients map[*Client]struct{}

	wg sync.WaitGroup
}

func NewGateway() *Gateway {
	return &Gateway{
		clients: make(map[*Client]struct{}),
	}
}

func (g *Gateway) register(client *Client) {
	g.mu.Lock()
	g.clients[client] = struct{}{}
	g.mu.Unlock()

	fmt.Printf("[ПІДКЛЮЧЕННЯ] Новий клієнт: %s\n", client.conn.RemoteAddr())
}

func (g *Gateway) unregister(client *Client) {
	g.mu.Lock()

	if _, exists := g.clients[client]; exists {
		delete(g.clients, client)
	}

	g.mu.Unlock()
}

func (g *Gateway) clientsExcept(excluded *Client) []*Client {
	g.mu.RLock()
	defer g.mu.RUnlock()

	list := make([]*Client, 0, len(g.clients))

	for client := range g.clients {
		if client != excluded {
			list = append(list, client)
		}
	}

	return list
}

func (g *Gateway) broadcast(message string, excluded *Client) {
	recipients := g.clientsExcept(excluded)

	for _, client := range recipients {
		if err := client.send(message); err != nil {
			fmt.Printf(
				"[ПОМИЛКА] Не вдалося відправити повідомлення %s: %v\n",
				client.conn.RemoteAddr(),
				err,
			)
		}
	}
}

func (g *Gateway) countClients() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.clients)
}

func (g *Gateway) startTCP() error {
	listener, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		return err
	}

	g.tcp = listener

	fmt.Println("[СЕРВЕР] TCP-слухач відкрито на порту 9000")

	g.wg.Add(1)

	go func() {
		defer g.wg.Done()
		g.acceptLoop()
	}()

	return nil
}

func (g *Gateway) acceptLoop() {
	for {
		conn, err := g.tcp.Accept()
		if err != nil {
			return
		}

		client := &Client{
			conn: conn,
		}

		g.register(client)

		g.wg.Add(1)

		go func() {
			defer g.wg.Done()
			g.handleClient(client)
		}()

		g.broadcast(
			fmt.Sprintf(
				"[СИСТЕМА] Підключився новий клієнт: %s",
				conn.RemoteAddr(),
			),
			client,
		)
	}
}

func (g *Gateway) handleClient(client *Client) {
	defer func() {
		g.unregister(client)
		client.conn.Close()

		g.broadcast(
			fmt.Sprintf(
				"[СИСТЕМА] Клієнт відключився: %s",
				client.conn.RemoteAddr(),
			),
			nil,
		)

		fmt.Printf(
			"[ВІДКЛЮЧЕННЯ] Клієнт: %s\n",
			client.conn.RemoteAddr(),
		)
	}()

	client.send("[СЕРВЕР] Ласкаво просимо до мережевого сервісу диспетчеризації!")
	client.send("[СЕРВЕР] Доступні команди: ECHO, TIME, BROADCAST, MSG, CLOSE, QUIT")

	reader := bufio.NewReader(client.conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		command := strings.TrimSpace(line)

		if command == "" {
			continue
		}

		if !g.processCommand(client, command) {
			return
		}
	}
}

func (g *Gateway) processCommand(client *Client, input string) bool {
	fields := strings.SplitN(input, " ", 2)
	command := strings.ToUpper(fields[0])

	var argument string

	if len(fields) == 2 {
		argument = strings.TrimSpace(fields[1])
	}

	switch command {
	case "ECHO":
		if argument == "" {
			client.send("[ПОМИЛКА] Використання: ECHO <повідомлення>")
			return true
		}

		client.send("[ECHO]: " + argument)

	case "TIME":
		client.send("[TIME]: " + time.Now().Format(time.RFC3339))

	case "BROADCAST", "MSG":
		if argument == "" {
			client.send("[ПОМИЛКА] Використання: BROADCAST <повідомлення>")
			return true
		}

		message := fmt.Sprintf(
			"[СПОВІЩЕННЯ від %s]: %s",
			client.conn.RemoteAddr(),
			argument,
		)

		fmt.Printf(
			"[РОЗСИЛКА] Від %s -> %q\n",
			client.conn.RemoteAddr(),
			argument,
		)

		g.broadcast(message, client)

	case "CLOSE", "QUIT":
		client.send("[СЕРВЕР] З'єднання буде закрито. До побачення!")
		return false

	default:
		client.send(
			"[ПОМИЛКА] Невідома команда. Доступні: ECHO, TIME, BROADCAST, MSG, CLOSE, QUIT",
		)
	}

	return true
}

func (g *Gateway) startUDP() error {
	conn, err := net.ListenPacket("udp", udpAddress)
	if err != nil {
		return err
	}

	g.udp = conn

	fmt.Println("[СЕРВЕР] UDP-слухач телеметрії відкрито на порту 9001")

	g.wg.Add(1)

	go func() {
		defer g.wg.Done()
		g.udpLoop()
	}()

	return nil
}

func (g *Gateway) udpLoop() {
	buffer := make([]byte, 4096)

	for {
		count, sender, err := g.udp.ReadFrom(buffer)
		if err != nil {
			return
		}

		payload := strings.TrimSpace(string(buffer[:count]))

		if payload == "" {
			continue
		}

		fmt.Printf(
			"[UDP ТЕЛЕМЕТРІЯ] Отримано %d байт від %s -> %q\n",
			count,
			sender,
			payload,
		)

		message := fmt.Sprintf(
			"[TELEMETRY from %s]: %s",
			sender,
			payload,
		)

		g.broadcast(message, nil)
	}
}

func (g *Gateway) closeClients() int {
	g.mu.RLock()
	active := make([]*Client, 0, len(g.clients))

	for client := range g.clients {
		active = append(active, client)
	}

	g.mu.RUnlock()

	for _, client := range active {
		client.send("[СИСТЕМА]: SERVER_SHUTTING_DOWN")
		client.conn.Close()
	}

	return len(active)
}

func (g *Gateway) shutdown() {
	fmt.Println()
	fmt.Println("[СЕРВЕР] Отримано сигнал завершення роботи.")
	fmt.Println("[СЕРВЕР] Запуск Graceful Shutdown...")

	if g.tcp != nil {
		g.tcp.Close()
	}

	if g.udp != nil {
		g.udp.Close()
	}

	count := g.closeClients()

	fmt.Printf(
		"[СЕРВЕР] Сповіщено активних клієнтів: %d. З'єднання закриті.\n",
		count,
	)

	g.wg.Wait()

	fmt.Println("[СЕРВЕР] Усі серверні горутини завершено.")
	fmt.Println("[СЕРВЕР] Роботу сервера успішно завершено.")
}

func main() {
	fmt.Println("=== Мережевий сервіс диспетчеризації (TCP: 9000, UDP: 9001) ===")
	fmt.Println()

	gateway := NewGateway()

	if err := gateway.startTCP(); err != nil {
		fmt.Printf("[ПОМИЛКА] TCP: %v\n", err)
		return
	}

	if err := gateway.startUDP(); err != nil {
		fmt.Printf("[ПОМИЛКА] UDP: %v\n", err)
		gateway.tcp.Close()
		gateway.wg.Wait()
		return
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals

	signal.Stop(signals)
	gateway.shutdown()
}
