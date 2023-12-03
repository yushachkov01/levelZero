package service

import (
	"fmt"
	"golang_project/internal/data"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/nats-io/stan.go"
	//"golang_project/internal/data"
)

// OrderService содержит логику работы с заказами
type OrderService struct {
	cache map[string]string
	db    *pgx.Conn
	sc    stan.Conn
}

// NewOrderService создает новый экземпляр OrderService
func NewOrderService(db *pgx.Conn, sc stan.Conn) *OrderService {
	return &OrderService{
		cache: make(map[string]string),
		db:    db,
		sc:    sc,
	}
}

// Start запускает сервис обработки заказов
func (so *OrderService) Start() {
	so.cache["name"] = "Alice"
	data.RestoreCacheFromDB(so.db)
	so.setupNatsSubscription()
	so.setupHTTPHandlers()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT)

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Ошибка при запуске HTTP-сервера: %v", err)
		}
	}()

	<-signalChan
	fmt.Println("Завершение работы...")
	time.Sleep(1 * time.Second)
}

func (os *OrderService) setupNatsSubscription() {
	channel := "example-channel"
	sub, err := os.sc.Subscribe(channel, func(msg *stan.Msg) {
		os.cache["key"] = string(msg.Data)
	}, stan.DurableName("durable-name"))
	if err != nil {
		log.Fatalf("Ошибка подписки на канал: %v", err)
	}
	defer sub.Close()
}

func (os *OrderService) setupHTTPHandlers() {
	http.HandleFunc("/order/", os.getOrderDetailsByIDHandler)
}

func (os *OrderService) getOrderDetailsByIDHandler(w http.ResponseWriter, r *http.Request) {
	orderID := data.ExtractOrderID(r.URL.Path)
	if orderID == "" {
		http.Error(w, "Неверный путь", http.StatusBadRequest)
		return
	}

	orderDetails, found := data.GetOrderFromCache(orderID)
	if !found {
		http.Error(w, "Данные не найдены23", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(orderDetails))
}
