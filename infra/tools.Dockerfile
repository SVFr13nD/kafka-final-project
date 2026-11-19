FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y --no-install-recommends openssl python3 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
ENTRYPOINT ["python3", "scripts/configure.py"]
