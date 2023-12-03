package data

import (
	"context"
	"database/sql"
	"log"

	"github.com/jackc/pgx/v4"
)

// Определяем переменную cache в пакете data
var cache = make(map[string]string)

// Функция для восстановления кэша из базы данных
func restoreCacheFromDB(dbConn *pgx.Conn) {
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

		// Проверка на NULL перед сканированием в переменную типа string
		//if key.Valid {
		//	cache[key.String] = value.String
		//	log.Printf("Loaded data into cache: Key: %s, Value: %s\n", key.String, value.String)
		//}
	}
	if err := rows.Err(); err != nil {
		log.Printf("Ошибка при итерации по результатам запроса: %v", err)
	}
}

// Функция для получения данных из кэша по ключу
func GetFromCache(key string) (string, bool) {
	value, found := cache[key]
	return value, found
}

// Функция для добавления данных в кэш
func AddToCache(key, value string) {
	cache[key] = value
}

// Функция для сохранения данных в базу данных
func SaveToDB(dbConn *pgx.Conn, key, value string) error {
	_, err := dbConn.Exec(context.Background(), "INSERT INTO cache_table (key, value) VALUES ($1, $2)", key, value)
	if err != nil {
		log.Printf("Ошибка при сохранении данных в базу данных: %v", err)
		return err
	}
	return nil
}
