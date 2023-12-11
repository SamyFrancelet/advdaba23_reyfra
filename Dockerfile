FROM golang:1.21.5

# Set dest for COPY
WORKDIR /app

# Install dependencies
COPY app/go.mod ./
RUN go mod download

# Copy sources
COPY app/*.go ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /advdaba_reyfra .

# Run
CMD ["/advdaba_reyfra"]
