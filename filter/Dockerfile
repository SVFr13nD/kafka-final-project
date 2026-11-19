FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY filter/pom.xml .
RUN mvn -B dependency:go-offline
COPY filter/src ./src
RUN mvn -B package
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /src/target/product-filter-1.0.0.jar /app/filter.jar
CMD ["java", "-Xms128m", "-Xmx384m", "-jar", "/app/filter.jar"]
