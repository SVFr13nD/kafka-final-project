# Пример проверки

Это ожидаемый сценарий, а не логи выполненного запуска. Фактический вывод создают `demo.sh` и `verify.sh`.

1. В `data/products.jsonl` находятся p001–p006. Все записи соответствуют полному контракту товара из задания.
2. p006 запрещён до первой отправки. В PostgreSQL и отладочном файле должны быть p001–p005, p006 отсутствует.
3. Alice смотрит p001, добавляет его в корзину и покупает p002. Bob взаимодействует с p003 и p004.
4. После Spark первые две рекомендации Alice: `["p001", "p002"]`; Bob: `["p003", "p004"]`. Третья — из другой категории с нулевым баллом; это ожидаемое заполнение top-3.
5. После `ban add p001` запись p001 удаляется из PostgreSQL; CLIENT больше её не показывает. `ban remove p001` возвращает её без повторной загрузки магазина.
6. При остановке primary-3 остальные два брокера продолжают принимать записи. В `evidence/alerts.jsonl` появляется уведомление со статусом `firing`, после восстановления — `resolved`.

Посмотреть файл Kafka Connect:

```bash
docker compose exec -T connect cat /debug/products.jsonl
```

Посмотреть реплицированный топик (команда завершится по таймауту; код timeout не является ошибкой доставки):

```bash
docker compose exec -T secondary-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server secondary-1:9092 --consumer.config /certs/client.properties \
  --topic primary.products.filtered --from-beginning --timeout-ms 10000 \
  --property print.key=true
```

Посмотреть HDFS:

```bash
docker compose exec -T namenode hdfs dfs -ls -R /marketplace
docker compose exec -T namenode hdfs dfs -cat '/marketplace/recommendations/latest/part-*'
```

Проверить запросы и outbox:

```bash
docker compose exec -T postgres psql -U marketplace -c 'SELECT user_id,event_type,count(*) FROM client_requests GROUP BY 1,2 ORDER BY 1,2'
docker compose exec -T postgres psql -U marketplace -c 'SELECT count(*) AS pending FROM outbox WHERE published_at IS NULL'
```

Формат результата клиента: JSON с `request_id`, `user_id` и массивом `products`. UUID и время будут отличаться при каждом запуске. Примеры событий в `data/events.jsonl` показывают входные действия, а CLI добавляет уникальный `request_id` и время.
