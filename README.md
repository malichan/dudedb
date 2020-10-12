# DudeDB

## Run DudeDB

### Run with 1 node

```
./gradlew run
```

### Run with 4 nodes (using Docker)

```
docker build -t dudedb .
docker-compose up
```

## Interact with DudeDB

### Put a record

```
curl -i -X POST -d '{"first_name":"John","last_name":"Doe"}' 'http://localhost:8080/data/john'
```

### Get a record

```
curl -i -X GET 'http://localhost:8080/data/john'
```

### List records (optionally with a limit)

```
curl -i -X GET 'http://localhost:8080/data?limit=1'
```