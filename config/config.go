package config

type Config struct {
	Postgres PostgresConn `Validate:"required"`
}
type PostgresConn struct {
	Host     string
	Port     int
	User     string
	Password string
	Dbname   string
}
