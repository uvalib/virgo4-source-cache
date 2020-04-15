package main

import (
   "fmt"
   "log"

   dbx "github.com/go-ozzo/ozzo-dbx"
   _ "github.com/lib/pq"
)

type cacheService struct {
   handle *dbx.DB
   table  string
   size   int
}

// and the factory
func NewDbCache(id int, cfg ServiceConfig) *cacheService {

   // connect to database
   log.Printf("[main] creating postgres connection %d", id)

   connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d sslmode=disable",
      cfg.PostgresUser, cfg.PostgresPass, cfg.PostgresDatabase, cfg.PostgresHost, cfg.PostgresPort, 30)

   db, err := dbx.MustOpen("postgres", connStr)
   if err != nil {
      log.Fatal(err)
   }

   return &cacheService{
      handle: db,
      table:  cfg.PostgresTable,
      size:   cfg.PostgresBatchSize,
   }
}
