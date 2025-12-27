# ---------- BUILD ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -q -DskipTests package

# ---------- RUNTIME ----------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/gdd/target/vn.bnh.gdd-1.0.0.jar /app/app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
