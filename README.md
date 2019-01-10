# siren
siren is a cryptocurrency exchange built using microservice architecture.

The main components are:
* grpc
* postgres
* redis
* kafka as message bus
* in-memory matching engine based on skiplist
* auth0 service for authentication

The design of this project borrows from [viabtc_exchange_server](https://github.com/viabtc/viabtc_exchange_server), and can work with a front-end SPA (not open sourced) that integrates tradingview.

This project only implemented core functions and has not been fully tested.
