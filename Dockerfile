FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
RUN mvn -q -DskipTests -pl gdd -am package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/gdd/target/vn.bnh.gdd-1.0.0.jar /app/app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
