FROM golang:1.19.6

WORKDIR /app

ADD . /app

RUN go mod download

RUN go build -o yuzu-feed-handler

CMD ["./yuzu-feed-handler"]

EXPOSE 8081
