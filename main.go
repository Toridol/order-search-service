package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
	stan "github.com/nats-io/stan.go"
)

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDT    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	RID         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type Order struct {
	OrderUID        string   `json:"order_uid"`
	TrackNumber     string   `json:"track_number"`
	Entry           string   `json:"entry"`
	Delivery        Delivery `json:"delivery"`
	Payment         Payment  `json:"payment"`
	Items           []Item   `json:"items"`
	Locale          string   `json:"locale"`
	InternalSign    string   `json:"internal_signature"`
	CustomerID      string   `json:"customer_id"`
	DeliveryService string   `json:"delivery_service"`
	ShardKey        string   `json:"shardkey"`
	SmID            int      `json:"sm_id"`
	DateCreated     string   `json:"date_created"`
	OofShard        string   `json:"oof_shard"`
}

type SafeCache struct {
	sync.RWMutex
	data map[string]Order
}

var cache = SafeCache{data: make(map[string]Order)}

func main() {
	par_podkl := "host=localhost port=5432 user=postgres password=0862v dbname=orders sslmode=disable"
	bd, err := sql.Open("pgx", par_podkl)
	if err != nil {
		log.Fatal("❌ Ошибка подключения к БД:", err)
	}
	defer bd.Close()

	if err := bd.Ping(); err != nil {
		log.Fatal("❌ БД недоступна:", err)
	}
	log.Println("✅ Подключено к PostgreSQL")

	rows, err := bd.Query(`SELECT order_uid, track_number, entry, locale, internal_signature, customer_id,
        delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders`)
	if err != nil {
		log.Fatal("Ошибка при загрузке заказов из БД:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var o Order
		err := rows.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSign,
			&o.CustomerID, &o.DeliveryService, &o.ShardKey, &o.SmID, &o.DateCreated, &o.OofShard)
		if err != nil {
			log.Println("Ошибка при сканировании строки:", err)
			continue
		}

		err = bd.QueryRow(`SELECT name, phone, zip, city, address, region, email 
                       FROM delivery WHERE order_uid=$1`, o.OrderUID).
			Scan(&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City,
				&o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
		if err != nil {
			log.Println("Ошибка при загрузке delivery:", err)
		}

		err = bd.QueryRow(`SELECT transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
                       FROM payment WHERE order_uid=$1`, o.OrderUID).
			Scan(&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency, &o.Payment.Provider,
				&o.Payment.Amount, &o.Payment.PaymentDT, &o.Payment.Bank, &o.Payment.DeliveryCost,
				&o.Payment.GoodsTotal, &o.Payment.CustomFee)
		if err != nil {
			log.Println("Ошибка при загрузке payment:", err)
		}

		itemsRows, err := bd.Query(`SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status 
                                FROM items WHERE order_uid=$1`, o.OrderUID)
		if err != nil {
			log.Println("Ошибка при загрузке items:", err)
		} else {
			for itemsRows.Next() {
				var it Item
				if err := itemsRows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.RID, &it.Name, &it.Sale,
					&it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
					log.Println("Ошибка сканирования item:", err)
					continue
				}
				o.Items = append(o.Items, it)
			}
			itemsRows.Close()
		}

		cache.Lock()
		cache.data[o.OrderUID] = o
		cache.Unlock()
	}
	log.Printf("📩 Кэш загружен: %d заказов\n", len(cache.data))

	sc, err := stan.Connect("test-cluster", "client-123")
	if err != nil {
		log.Fatal("❌ Ошибка подключения к NATS:", err)
	}
	defer sc.Close()
	log.Println("✅ Подключено к NATS")

	_, err = sc.Subscribe("orders", func(m *stan.Msg) {
		log.Printf("📦 Получено сообщение: %s", string(m.Data))

		var order Order
		if err := json.Unmarshal(m.Data, &order); err != nil {
			log.Println("❌ Ошибка разбора JSON:", err)
			return
		}

		if order.OrderUID == "" || len(order.Items) == 0 {
			log.Println("⚠️ Некорректный заказ (нет ID или items), пропускаем")
			return
		}

		if err := saveOrder(bd, &order); err != nil {
			log.Println("❌ Ошибка при сохранении в БД:", err)
			return
		}
		log.Println("✅ Заказ сохранён в БД:", order.OrderUID)

		if err := m.Ack(); err != nil {
			log.Println("⚠️ Ошибка при подтверждении сообщения:", err)
		}

	}, stan.DeliverAllAvailable(), stan.SetManualAckMode())
	if err != nil {
		log.Fatal("Ошибка подписки:", err)
	}

	log.Println("🟢 Слушаем канал 'orders'... Ctrl+C для выхода")

	go func() {
		http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, "не указан параметр id", http.StatusBadRequest)
				return
			}
			cache.RLock()
			order, ok := cache.data[id]
			cache.RUnlock()
			if !ok {
				http.Error(w, "заказ не найден в кэше", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(order)
		})

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.ServeFile(w, r, "inter.html")
				return
			}
			http.NotFound(w, r)
		})

		http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

		log.Println("🌐 HTTP сервер запущен на :8080 (http://localhost:8080)")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	select {}
}

func saveOrder(db *sql.DB, order *Order) error {
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM orders WHERE order_uid=$1)", order.OrderUID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		log.Println("⚠️ Заказ уже существует в БД:", order.OrderUID)
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id,
		                  delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, order.OrderUID, order.TrackNumber, order.Entry, order.Locale, order.InternalSign,
		order.CustomerID, order.DeliveryService, order.ShardKey, order.SmID, order.DateCreated, order.OofShard)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO delivery (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, order.OrderUID, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO payment (order_uid, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, order.OrderUID, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency, order.Payment.Provider,
		order.Payment.Amount, order.Payment.PaymentDT, order.Payment.Bank, order.Payment.DeliveryCost,
		order.Payment.GoodsTotal, order.Payment.CustomFee)
	if err != nil {
		return err
	}

	for _, item := range order.Items {
		_, err = tx.Exec(`
			INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`, order.OrderUID, item.ChrtID, item.TrackNumber, item.Price, item.RID, item.Name, item.Sale,
			item.Size, item.TotalPrice, item.NmID, item.Brand, item.Status)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	cache.Lock()
	cache.data[order.OrderUID] = *order
	cache.Unlock()
	log.Println("👍🏻 Добавлен в кэш:", order.OrderUID)

	return nil
}
