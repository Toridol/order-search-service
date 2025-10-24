package main

import (
	"log"

	stan "github.com/nats-io/stan.go"
)

func main() {
	sc, err := stan.Connect("test-cluster", "publisher-1")
	if err != nil {
		log.Fatal("Ошибка подключения к NATS:", err)
	}
	defer sc.Close()

	msg := `{
		"order_uid":"test2",
		"track_number":"WBILMTESTTRACK",
		"entry":"WBIL",
		"delivery":{
		 "name":"Иван Иванов",
		 "phone":"+9520000000",
		 "zip":"2639809",
		 "city":"Москва",
		 "address":"Пушкина 15",
		 "region":"Kraiot",
		 "email":"test@gmail.com"},
		 "payment":{
		  "transaction":"b563feb7b2b84b6test2",
		  "request_id":"",
		  "currency":"USD",
		  "provider":"wbpay",
		  "amount":1317,
		  "payment_dt":1637907727,
		  "bank":"alpha",
		  "delivery_cost":1500,
		  "goods_total":2817,
		  "custom_fee":0},
		"items":[
		 {"chrt_id":9934934,
		  "track_number":"WBILMTESTTRACK",
		  "price":458,
		  "rid":"ab4219087a764ae0btest",
		  "name":"Mascarass",
		  "sale":30,
		  "size":"0",
		  "total_price":317,
		  "nm_id":2389212,
		  "brand":"Vivienne Sabo",
		  "status":202},
		  {"chrt_id":9934935,
		  "track_number":"WBILMTESTTRACK",
		  "price":1000,
		  "rid":"ab4219087a764ae0btest2",
		  "name":"Shampoo",
		  "sale":0,
		  "size":"0",
		  "total_price":1000,
		  "nm_id":2389213,
		  "brand":"Limbo",
		  "status":202}
		 ],
		"locale":"ru",
		"internal_signature":"",
		"customer_id":"test1",
		"delivery_service":"meest",
		"shardkey":"9",
		"sm_id":99,
		"date_created":"2025-12-20T17:20:19Z",
		"oof_shard":"1"
	}`

	err = sc.Publish("orders", []byte(msg))
	if err != nil {
		log.Fatal("❌ Ошибка при публикации:", err)
	}

	log.Println("✅ Сообщение отправлено в канал 'orders'")
}
