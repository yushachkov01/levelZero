package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/prashanthpai/sqlcache"
	"golang_project/internal/handlers"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/nats-io/stan.go"

	"golang_project/internal/data"
	_ "golang_project/internal/handlers"
)

func main() {

	//новое отсюда
	var rc = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
	})

	// create a sqlcache.Interceptor instance with the desired backend
	var interceptor, err = sqlcache.NewInterceptor(&sqlcache.Config{
		Cache: sqlcache.NewRedis(rc, "sqc"),
	})
	sql.Register("pgx-with-cache", interceptor.Driver(stdlib.GetDefaultDriver()))
	// до сюда
	connString := "postgresql://yushachkov:1234@localhost:5432/levelzerodb"
	dbConn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		log.Fatalf("Ошибка подключения к PostgreSQL: %v", err)
	}
	defer dbConn.Close(context.Background())

	jsonData := `{
	"order_uid": "b563feb7b2b84b6test",
		"track_number": "WBILMTESTTRACK",
		"entry": "WBIL",
		"delivery": {
		"name": "Test Testov",
			"phone": "+9720000000",
			"zip": "2639809",
			"city": "Kiryat Mozkin",
			"address": "Ploshad Mira 15",
			"region": "Kraiot",
			"email": "test@gmail.com"
		},
	"payment": {
		"transaction": "b563feb7b2b84b6test",
			"request_id": "",
			"currency": "USD",
			"provider": "wbpay",
			"amount": 1817,
			"payment_dt": 1637907727,
			"bank": "alpha",
			"delivery_cost": 1500,
			"goods_total": 317,
			"custom_fee": 0
	},
	"items": [
{
"chrt_id": 9934930,
"track_number": "WBILMTESTTRACK",
"price": 453,
"rid": "ab4219087a764ae0btest",
"name": "Mascaras",
"sale": 30,
"size": "0",
"total_price": 317,
"nm_id": 2389212,
"brand": "Vivienne Sabo",
"status": 202
}
],
"locale": "en",
"internal_signature": "",
"customer_id": "test",
"delivery_service": "meest",
"shardkey": "9",
"sm_id": 99,
"date_created": "2021-11-26T06:22:19Z",
"oof_shard": "1"
}`

	var orderData data.Order
	err = json.Unmarshal([]byte(jsonData), &orderData)
	if err != nil {
		panic(err)
	}

	query := `
	INSERT INTO orders (
		order_uid, track_number, entry, 
		delivery_name, delivery_phone, delivery_zip, delivery_city, delivery_address, delivery_region, delivery_email,
		payment_transaction, payment_request_id, payment_currency, payment_provider, payment_amount, payment_payment_dt, payment_bank, payment_delivery_cost, payment_goods_total, payment_custom_fee,
		items_chrt_id, items_track_number, items_price, items_rid, items_name, items_sale, items_size, items_total_price, items_nm_id, items_brand, items_status,
		locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
	) 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39)
`

	// Внесение тестовых данных в базу данных
	_, err = dbConn.Exec(context.Background(), query, orderData.OrderUID, orderData.TrackNumber, orderData.Entry,
		orderData.Delivery.Name, orderData.Delivery.Phone, orderData.Delivery.Zip, orderData.Delivery.City, orderData.Delivery.Address, orderData.Delivery.Region, orderData.Delivery.Email,
		orderData.Payment.Transaction, orderData.Payment.RequestID, orderData.Payment.Currency, orderData.Payment.Provider, orderData.Payment.Amount, orderData.Payment.PaymentDt, orderData.Payment.Bank, orderData.Payment.DeliveryCost, orderData.Payment.GoodsTotal, orderData.Payment.CustomFee,
		orderData.Items[0].ChrtID, orderData.Items[0].TrackNumber, orderData.Items[0].Price, orderData.Items[0].RID, orderData.Items[0].Name, orderData.Items[0].Sale, orderData.Items[0].Size, orderData.Items[0].TotalPrice, orderData.Items[0].NmID, orderData.Items[0].Brand, orderData.Items[0].Status,
		orderData.Locale, orderData.InternalSig, orderData.CustomerID, orderData.DeliveryServ, orderData.Shardkey, orderData.SmID, orderData.DateCreated, orderData.OofShard)
	if err != nil {
		log.Fatalf("Ошибка при внесении данных в базу данных: %v", err)
	}

	log.Println("Тестовые данные успешно добавлены в базу данных.")
	time.Sleep(1 * time.Second)

	data.RestoreCacheFromDB(dbConn)

	clusterID := "test-cluster"
	clientID := "client-1"
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL("nats://localhost:4222"))
	if err != nil {
		log.Fatalf("Ошибка подключения к серверу NATS Streaming: %v", err)
	}
	defer sc.Close()
	// Добавляем единственный обработчик для /order/
	http.HandleFunc("/order/", orderHandler)

	//jsonData, ok := cache["jsonData"]
	//if !ok {
	//	http.Error(w, "Ошибка получения данных из кэша --->>", http.StatusInternalServerError)
	//	return
	//}

	//Проверка типа данных и преобразование в строку
	//dataString, ok := jsonData.(string)
	//if !ok {
	//	http.Error(w, "Ошибка получения данных из кэша", http.StatusInternalServerError)
	//	return
	//}

	// Преобразование строки в объект data.Order
	//var orderData data.Order
	//err := json.Unmarshal([]byte(jsonData), &orderData)
	//if err != nil {
	//	http.Error(w, "Ошибка разбора данных JSON", http.StatusInternalServerError)
	//	return
	//}
	// Возврат данных из jsonData

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Ошибка при запуске HTTP-сервера: %v", err)
		}
	}()

	<-signalChan
	fmt.Println("Завершение работы...")
	time.Sleep(1 * time.Second)

}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	// Устанавливаем заголовки CORS для разрешения запросов с любого источника
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "GET" {
		// Обработка GET запроса для получения деталей заказа
		handlers.GetOrderDetailsByIDHandler(w, r)
	} else if r.Method == "POST" {
		// Обработка POST запроса для создания нового заказа
		// your logic here for POST request
	} else if r.Method == "OPTIONS" {
		// Обработка запроса метода OPTIONS для предоставления прав доступа
		w.WriteHeader(http.StatusOK)
	} else {
		// Если метод запроса не поддерживается
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}
