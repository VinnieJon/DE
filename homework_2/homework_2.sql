--вывести количество фильмов в каждой категории, отсортировать по убыванию.
SELECT c.name,
       count(fc.film_id) AS movies_count
FROM category c
LEFT JOIN film_category fc ON c.category_id = fc.category_id
GROUP BY c.name
ORDER BY movies_count DESC

--вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

SELECT concat(a.first_name, ' ', a.last_name) AS actor,
       count(DISTINCT r.rental_id) AS rental_count
FROM rental r
LEFT JOIN inventory i ON r.inventory_id=i.inventory_id
LEFT JOIN film_actor fa ON i.film_id=fa.film_id
LEFT JOIN actor a ON fa.actor_id=a.actor_id
GROUP BY actor
ORDER BY rental_count DESC
LIMIT 10


--вывести категорию фильмов, на которую потратили больше всего денег.

SELECT c.name,
       sum(p.amount) AS rental_sum
FROM payment p
LEFT JOIN rental r ON p.rental_id=r.rental_id
LEFT JOIN inventory i ON r.inventory_id=i.inventory_id
LEFT JOIN film_category fc ON i.film_id=fc.film_id
LEFT JOIN category c ON fc.category_id=c.category_id
GROUP BY c.name
ORDER BY rental_sum DESC
LIMIT 1

--вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

SELECT f.title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id IS NULL

--вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
SELECT *
FROM
  (SELECT actor,
          count_movies,
          rank() OVER (
                       ORDER BY count_movies DESC) AS actor_rank
   FROM
     (SELECT concat(a.first_name, ' ', a.last_name) AS actor,
             count(fa.film_id) AS count_movies
      FROM film_actor fa
      LEFT JOIN actor a ON a.actor_id=fa.actor_id
      LEFT JOIN film_category fc ON fa.film_id=fc.film_id
      LEFT JOIN category c ON fc.category_id=c.category_id
      WHERE c.name = 'Children'
      GROUP BY actor
      ORDER BY count_movies DESC) AS actor_movies) AS rank_table
WHERE actor_rank <= 3

--вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
SELECT ct.city,
       count(CASE
                 WHEN c.active = 1 THEN 'Active'
                 ELSE NULL
             END) AS active,
       count(CASE
                 WHEN c.active = 0 THEN 'No active'
                 ELSE NULL
             END) AS no_active
FROM customer c
LEFT JOIN address a ON c.address_id = a.address_id
LEFT JOIN city ct ON a.city_id = ct.city_id
GROUP BY ct.city
ORDER BY no_active DESC

--вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
SELECT DISTINCT c.name,
                sum((date_part('month', return_date::TIMESTAMP) - date_part('month', rental_date::TIMESTAMP))*720 + (date_part('day', return_date::TIMESTAMP) - date_part('day', rental_date::TIMESTAMP))*24 +(date_part('hour', return_date::TIMESTAMP) - date_part('hour', rental_date::TIMESTAMP))) AS hours_sum
FROM rental r
LEFT JOIN inventory i ON r.inventory_id=i.inventory_id
LEFT JOIN film_category fc ON i.film_id=fc.film_id
LEFT JOIN category c ON fc.category_id=c.category_id
LEFT JOIN customer cus ON r.customer_id=cus.customer_id
LEFT JOIN address a ON cus.address_id = a.address_id
LEFT JOIN city ct ON a.city_id = ct.city_id
WHERE date_part('year', rental_date::TIMESTAMP) = '2005'
  AND (ct.city like 'a%'
       OR ct.city like 'A%'
       OR ct.city like '%-%'
       OR ct.city like '-%'
       OR ct.city like '%-')
GROUP BY c.name
ORDER BY hours_sum DESC


