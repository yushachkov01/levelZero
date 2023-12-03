package data

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4"
	"log"
)

var ctx = context.Background()

func WriteOrderToRedis(dbConn *pgx.Conn) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	rows, err := dbConn.Query(context.Background(), "SELECT order_id, order_data FROM orders")
	if err != nil {
		log.Fatalf("Ошибка при выполнении запроса к базе данных: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var orderID, orderData string
		err := rows.Scan(&orderID, &orderData)
		if err != nil {
			log.Printf("Ошибка при сканировании данных из базы данных: %v", err)
			continue
		}

		// Запись данных в Redis
		err = client.Set(ctx, "order:"+orderID, orderData, 0).Err()
		if err != nil {
			log.Fatalf("Ошибка при записи данных в Redis: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("Ошибка при итерации по результатам запроса: %v", err)
	}

	log.Println("Данные успешно записаны в Redis.")
}
