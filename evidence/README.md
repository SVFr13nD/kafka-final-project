# Материалы проверки

После `start.sh`, `demo.sh`, `verify.sh` здесь появятся реальные результаты. Секреты и сертификаты сюда не копируются.

- `demo.log`: отправка товаров, запросы и рекомендации.
- `verification.log`: положительные и отрицательные проверки.
- `topology.json`, `primary-topics.txt`, `secondary-topics.txt`: три партиции, RF=3, ISR.
- `mirrored-products.json`: данные во втором Kafka-кластере.
- `recommendations.json`: результат аналитики.
- `alerts.jsonl`: полученные webhook-уведомления Alertmanager, включая resolved.
- `services.txt`, `services.log`: контейнеры и логи обработчиков.
- В CI дополнительно: `compose-status.txt`, `compose.log`.

Для сдачи приложите скриншоты Grafana с шестью брокерами, `docker compose ps`, успешного `demo.sh` и финальной строки `ALL INTEGRATION CHECKS PASSED`. Скриншоты делают после фактического запуска, не из заранее подготовленного текста.
