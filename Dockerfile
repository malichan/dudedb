FROM adoptopenjdk/openjdk11:alpine AS build
WORKDIR /dudedb
COPY . .
RUN ./gradlew distZip

FROM adoptopenjdk/openjdk11:alpine-jre
WORKDIR /
COPY --from=build /dudedb/build/distributions/dudedb-1.0.zip .
RUN unzip dudedb-1.0.zip
CMD ["/dudedb-1.0/bin/dudedb"]
