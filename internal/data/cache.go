package data

import (
	"context"
	"database/sql"
	"log"
	"strings"

	"github.com/jackc/pgx/v4"
)

var cacheMap = make(map[string]string)

func GetCache() map[string]string {
	return cacheMap
}

// RestoreCacheFromDB восстанавливает кэш из базы данных
func RestoreCacheFromDB(dbConn *pgx.Conn) {
	rows, err := dbConn.Query(context.Background(), "SELECT key, value FROM cache_table")
	if err != nil {
		log.Printf("Ошибка при загрузке кэша из базы данных: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var key, value sql.NullString
		err := rows.Scan(&key, &value)
		if err != nil {
			log.Printf("Ошибка при сканировании данных из базы данных: %v", err)
			continue
		}

		if key.Valid {
			cacheMap[key.String] = value.String
			log.Printf("Loaded data into cache: Keyss: %s, Value: %s\n", key.String, value.String)
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("Ошибка при итерации по результатам запроса: %v", err)
	}
}

// GetOrderDetailsByID получает детали заказа по его ID из кэша
func GetOrderDetailsByID(orderID string) (string, bool) {
	orderDetails, found := cacheMap[orderID]
	return orderDetails, found
}

// SetOrderDetails устанавливает детали заказа в кэше
func SetOrderDetails(orderID, orderDetails string) {
	cacheMap[orderID] = orderDetails
}

// ExtractOrderID извлекает ID заказа из URL
func ExtractOrderID(urlPath string) string {
	parts := strings.Split(urlPath, "/")
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}
