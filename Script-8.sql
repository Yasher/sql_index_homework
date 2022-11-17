# Исходный запрос

	select
		distinct concat(c.last_name, ' ', c.first_name) ,
		sum(p.amount) over (partition by c.customer_id,
		f.title)
	from
		payment p,
		rental r,
		customer c,
		inventory i,
		film f
	where
		date(p.payment_date) = '2005-07-30'
		and p.payment_date = r.rental_date
		and r.customer_id = c.customer_id
		and i.inventory_id = r.inventory_id
		
		        -> Limit: 200 row(s)  (cost=0.00..0.00 rows=0) (actual time=5603.963..5603.994 rows=200 loops=1)
    -> Table scan on <temporary>  (cost=2.50..2.50 rows=0) (actual time=5603.961..5603.984 rows=200 loops=1)
        -> Temporary table with deduplication  (cost=2.50..2.50 rows=0) (actual time=5603.959..5603.959 rows=391 loops=1)
            -> Window aggregate with buffering: sum(payment.amount) OVER (PARTITION BY c.customer_id,f.title )   (actual time=2162.952..5417.171 rows=642000 loops=1)
                -> Sort: c.customer_id, f.title  (actual time=2162.917..2225.363 rows=642000 loops=1)
                    -> Stream results  (cost=21711647.46 rows=16009975) (actual time=1.306..1612.494 rows=642000 loops=1)
                        -> Nested loop inner join  (cost=21711647.46 rows=16009975) (actual time=1.290..1342.567 rows=642000 loops=1)
                            -> Nested loop inner join  (cost=20106647.44 rows=16009975) (actual time=1.281..1189.859 rows=642000 loops=1)
                                -> Nested loop inner join  (cost=18501647.43 rows=16009975) (actual time=1.265..1026.402 rows=642000 loops=1)
                                    -> Inner hash join (no condition)  (cost=1581480.53 rows=15813000) (actual time=1.231..59.144 rows=634000 loops=1)
                                        -> Filter: (cast(p.payment_date as date) = '2005-07-30')  (cost=1.65 rows=15813) (actual time=0.112..6.130 rows=634 loops=1)
                                            -> Table scan on p  (cost=1.65 rows=15813) (actual time=0.075..4.342 rows=16044 loops=1)
                                        -> Hash
                                            -> Covering index scan on f using idx_title  (cost=108.73 rows=1000) (actual time=0.097..0.833 rows=1000 loops=1)
                                    -> Covering index lookup on r using rental_date (rental_date=p.payment_date)  (cost=0.97 rows=1) (actual time=0.001..0.001 rows=1 loops=634000)
                                -> Single-row index lookup on c using PRIMARY (customer_id=r.customer_id)  (cost=0.00 rows=1) (actual time=0.000..0.000 rows=1 loops=642000)
                            -> Single-row covering index lookup on i using PRIMARY (inventory_id=r.inventory_id)  (cost=0.00 rows=1) (actual time=0.000..0.000 rows=1 loops=642000)

		
# Оптимизированый запрос

	SELECT
		CONCAT( c.last_name , ' ', c.first_name) name,
		SUM(p.amount)
	FROM
		payment p
	#JOIN rental r ON
	#	r.rental_id = p.rental_id
	#JOIN inventory i ON
	#	i.inventory_id = r.inventory_id
	#JOIN film f ON
	#	f.film_id = i.film_id
	JOIN customer c ON
		c.customer_id = p.customer_id
	WHERE
		date(p.payment_date) = '2005-07-30'
	GROUP BY
		c.customer_id
	#ORDER BY
	#	name;
		
		             -> Limit: 200 row(s)  (actual time=29.343..29.368 rows=200 loops=1)
    -> Sort: `name`, limit input to 200 row(s) per chunk  (actual time=29.342..29.356 rows=200 loops=1)
        -> Stream results  (cost=7177.00 rows=15813) (actual time=0.957..29.030 rows=391 loops=1)
            -> Group aggregate: sum(p.amount)  (cost=7177.00 rows=15813) (actual time=0.935..28.655 rows=391 loops=1)
                -> Nested loop inner join  (cost=5595.70 rows=15813) (actual time=0.680..28.295 rows=634 loops=1)
                    -> Index scan on c using PRIMARY  (cost=61.15 rows=599) (actual time=0.196..0.551 rows=599 loops=1)
                    -> Filter: (cast(p.payment_date as date) = '2005-07-30')  (cost=6.60 rows=26) (actual time=0.042..0.046 rows=1 loops=599)
                        -> Index lookup on p using idx_fk_customer_id (customer_id=c.customer_id)  (cost=6.60 rows=26) (actual time=0.037..0.043 rows=27 loops=599)
		
	#Что сделал:
	# - заменил соединение таблиц через WHERE на JOIN
	# - проверил и выявил, что payment_date  совпадает с rental_date, убрал выборку по rental_date
	# - в результате изначального запроса получается таблица, где выведены суммы платежей покупателей за опред. число,
	#	поэтому убрал из своего запроса JOINы с rental_id, inventory_id и film_id, т.к. данные из этих таблиц не нужны в результ. таблице
	#	и не влияют на результат
	# - результат проверял пересчетом строк обоих запросов (внешний запрос с COUNT(1)), также отсортировал результат по покупателям и 
	#	проверил идентичность нескольких строк результатов обоих запросов
	# - вывел explain analyze из которого видно, что оптимизированный запрос выполняется быстрее и с меньшей стоимостью
	#
	#
	#
	#